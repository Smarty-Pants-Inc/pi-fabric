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
import { residentRoot, type ResidentCommandResponse, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { ResidentMainGeneration, ResidentLaunchSpec } from "../src/residency/handover.js";
import { RESIDENT_DRAIN_ABI, residentDrainReceiptPath, type ResidentDrainReceipt } from "../src/residency/drain.js";
import type { ResidentDrainReceipt as CliReceipt } from "../src/resident-drain-cli.js";
import { launchLog, same } from "./helpers/owned-processes.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

// Keep: native trust boundary and accepted-work data safety (#11962). This is
// deliberately NOT an A->B success test. B is a separately retained real build,
// pinned and validated by the real endpoint, but cannot launch without containment.
// Only inference is substituted: native launcher -> Pi resident -> worker -> Pi
// inference, with deterministic loopback SSE. No recovery guard/transport is mocked.
const repo = fileURLToPath(new URL("../", import.meta.url));
const readJson = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
const live = (pid: number): boolean => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0] !== "Z";
  } catch { return false; }
};
type QueueItem = { id: string; source?: string; payload?: { id?: string }; attempts?: number; preparationAttempts?: number; launchEvidenceVersion?: number; executionStarted?: boolean };
type QueueSnapshot = { format: number; items: QueueItem[] };
type Cursor = { format: number; cursor: number; last?: { sequence: number; id: string } };
type CliResult = { receipt: CliReceipt; stdout: string; stderr: string; exit: { code: number | null; signal: NodeJS.Signals | null } };
type Accepted = { acknowledged: boolean; messageId?: string };
type Published = { id: string; sequence: number; createdAt: number };

// The normal observation source is the fixture's actual filesystem commits and
// child IPC/native exit. Timers below are finite failure bounds, never polling.
function observeFixture(root: string, capture: () => void) {
  const listeners = new Set<() => void>();
  let watchError: Error | undefined;
  const wake = () => { capture(); for (const listener of [...listeners]) listener(); };
  const watcher = fs.watch(root, { recursive: true, persistent: false }, wake);
  watcher.on("error", error => { watchError = error; wake(); });
  return {
    wake, close: () => watcher.close(),
    until: (test: () => boolean, what: string, ms = 60_000): Promise<void> => new Promise((resolve, reject) => {
      const finish = (error?: unknown) => {
        clearTimeout(timer); listeners.delete(check);
        if (error) reject(error); else resolve();
      };
      const check = () => {
        try { if (watchError) throw watchError; if (test()) finish(); }
        catch (error) { finish(error); }
      };
      const timer = setTimeout(() => finish(new Error(`Native cooperative drain deadline: ${what}`)), ms);
      listeners.add(check); check(); // Subscribe before reading.
    }),
  };
}

// Main is an actual separate participant/control runtime, not a sleeping PID or
// identity manufactured by the CLI. Compile the actual branch participant/control
// runtime into an owned fixture entry; mesh/client/worker/Pi use OLD's published dist.
// This small driver needs IPC only to exercise existing public operations and to
// let the lost-Main case join a real native exit without killing the test runner.
async function compileMain(root: string, oldRelease: string) {
  // Path-sensitive production helpers expect <install>/dist/<dir>/<entry>.
  // Preserve that layout in the owned driver instead of relocating import.meta
  // paths into /tmp. No old/target artifact is altered by this fixture compile.
  const runtimeRoot = path.join(root, "main-runtime");
  const mainEntry = path.join(runtimeRoot, "dist/fixture/native-main.mjs");
  fs.mkdirSync(path.dirname(mainEntry), { recursive: true, mode: 0o700 });
  fs.copyFileSync(path.join(oldRelease, "package.json"), path.join(runtimeRoot, "package.json"));
  fs.cpSync(path.join(oldRelease, "dist/native"), path.join(runtimeRoot, "dist/native"), { recursive: true });
  const nativeDigests = Object.fromEntries(fs.readdirSync(path.join(runtimeRoot, "dist/native")).flatMap(name => {
    const file = path.join(runtimeRoot, "dist/native", name);
    return fs.statSync(file).isFile() && !name.endsWith(".json")
      ? [[name, createHash("sha256").update(fs.readFileSync(file)).digest("hex")]] : [];
  }));
  // Retained proof releases need only the build, not a duplicate source checkout.
  const sourceRoot = fs.existsSync(path.join(oldRelease, "src/topology/participant-directory.ts")) ? oldRelease : repo;
  const source = (file: string) => JSON.stringify(path.join(sourceRoot, "src", file));
  const contents = `import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { ParticipantDirectory } from ${source("topology/participant-directory.ts")};
import { FabricControlPlane, controlOwnerIncarnation } from ${source("topology/control-plane.ts")};
import { residentLaunchSpec } from ${source("residency/handover.ts")};
import { processStartTime } from ${source("residency/process-identity.ts")};
import { writeJsonAtomic } from ${source("core/atomic-write.ts")};
const setup = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const { config, identity, oldRelease, targetRelease } = setup;
const { MeshStore } = await import(pathToFileURL(path.join(oldRelease, "dist/mesh.js")).href);
const { ResidentActorClient } = await import(pathToFileURL(path.join(oldRelease, "dist/residency/actor-client.js")).href);
const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
const startedAt = Date.now();
const birth = processStartTime(process.pid);
if (!birth) throw new Error("Main has no birth identity");
participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
  ownerHostId: identity.id, ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session",
  runner: "pi", transport: "host", capabilities: ["fabric", "followUp"], cwd: config.cwd, sessionId: identity.sessionId,
  startedAt, updatedAt: Date.now(), controlProtocol: "v1",
  mainProcess: { pid: process.pid, host: os.hostname(), startTime: birth } }]);
const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 30_000 });
const client = new ResidentActorClient(config.meshRoot, identity.id);
await participants.start(); control.start(() => ({ accepted: false }));
const main = { pid: process.pid, processStartTime: processStartTime(process.pid), nonce: randomUUID(),
  rootId: identity.id, sessionId: identity.sessionId, releaseRoot: oldRelease };
if (!main.processStartTime) throw new Error("Main has no birth identity");
// This write is owned by the live runtime AFTER real participant/control admission.
writeJsonAtomic(path.join(config.residencyRoot, "main-generation.json"), main, { durable: true });
writeJsonAtomic(path.join(config.cwd, "main-ready.json"), { main, caller: { identity, hostId: identity.id }, entry: process.argv[1] }, { durable: true });
process.send({ ready: true, main });
process.on("message", async request => {
  try {
    let result;
    if (request.operation === "create") result = await client.createActor(request.input);
    else if (request.operation === "status") result = await client.actorStatus(request.input.id);
    else if (request.operation === "followUp") {
      const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8"));
      const participant = participants.get(request.input.id, Date.now(), { fresh: true });
      const incarnation = participant && controlOwnerIncarnation(participant);
      if (!incarnation) throw new Error("Actor has no published live control incarnation");
      result = await control.request(owner.hostId, request.input.id, "followUp", { message: request.input.message,
        ownerIncarnation: incarnation }, owner.hostId);
    } else if (request.operation === "publish") {
      result = await mesh.publish({ topic: "native.drain.concurrent", from: identity, to: request.input.id,
        text: request.input.message, data: { proof: "concurrent-producer" } });
    } else if (request.operation === "drain") {
      const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8"));
      const targetConfig = { ...config, workerPath: path.join(targetRelease, "dist/worker.js"),
        fabricExtensionPath: path.join(targetRelease, "dist/index.js") };
      const target = residentLaunchSpec(targetConfig, path.join(targetRelease, "dist/residency/pi-entry.js"), process.execPath);
      result = await client.releaseDrain({ caller: { identity, hostId: identity.id }, main, target,
        expected: { releaseRoot: oldRelease, owner: { pid: owner.pid, processStartTime: owner.processStartTime,
          token: owner.token, hostId: owner.hostId }, launcher: owner.handover.launcher, configDigest: owner.configDigest },
        timeoutMs: request.input.timeoutMs });
    } else if (request.operation === "exit") {
      // Lost-Main probe: a real native exit, NOT a clean lineage-closure command
      // that independently asks the resident to stop session-owned actors.
      process.send({ id: request.id, result: { exited: true, main } }, () => process.exit(0)); return;
    } else throw new Error("Unknown fixture operation");
    process.send({ id: request.id, result });
  } catch (error) { process.send({ id: request.id, error: String(error && error.stack || error) }); }
});
`;
  const compiled = await build({ absWorkingDir: sourceRoot, stdin: { contents, resolveDir: sourceRoot, sourcefile: "native-main-driver.ts" }, outfile: mainEntry,
    bundle: true, packages: "external", platform: "node", format: "esm", target: "node24", sourcemap: true, metafile: true,
    define: { __FABRIC_NATIVE_DIGESTS__: JSON.stringify(JSON.stringify(nativeDigests)) }, logLevel: "silent" });
  // External dependencies resolve inside the retained OLD install, never the fleet.
  fs.symlinkSync(path.join(oldRelease, "node_modules"), path.join(runtimeRoot, "node_modules"), "dir");
  const sourceInputs = Object.keys(compiled.metafile!.inputs).filter(file => file !== "native-main-driver.ts" && !file.startsWith("<"));
  fs.writeFileSync(path.join(root, "main-compile.json"), JSON.stringify({ oldRelease, sourceRoot, mainEntry, nativeDigests,
    sha256: createHash("sha256").update(fs.readFileSync(mainEntry)).digest("hex"),
    sourceInputs: sourceInputs.map(file => ({ file, sha256: createHash("sha256").update(fs.readFileSync(path.resolve(sourceRoot, file))).digest("hex") })) }, null, 2));
  return mainEntry;
}

function mainRpc(child: ChildProcess, wake: () => void) {
  const pending = new Map<string, { resolve: (value: unknown) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  const fail = (error: Error) => {
    for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(error); }
    pending.clear(); wake();
  };
  child.on("error", fail);
  child.on("exit", () => fail(new Error("Native fixture Main exited")));
  child.on("message", (message: { id?: string; result?: unknown; error?: string }) => {
    wake();
    if (!message.id) return;
    const waiter = pending.get(message.id); if (!waiter) return;
    pending.delete(message.id); clearTimeout(waiter.timer);
    if (message.error) waiter.reject(new Error(message.error)); else waiter.resolve(message.result);
  });
  return <T>(operation: string, input: unknown = {}): Promise<T> => new Promise((resolve, reject) => {
    const id = randomUUID();
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`Native Main RPC deadline: ${operation}`)); }, 45_000);
    pending.set(id, { resolve: value => resolve(value as T), reject, timer });
    child.send({ id, operation, input }, error => { if (error) fail(error); });
  });
}

const cases = ["checkpoint-concurrent", "timeout", "lost-main", "legacy-aliased"] as const;
describe.skipIf(process.platform !== "linux")("compiled cooperative drain preserves A when containment is unavailable", () => {
  it.each(cases)("%s: accepted native work survives the real boundary exactly once", async mode => {
    const oldInput = process.env.OLD_RELEASE ?? process.env.FABRIC_DRAIN_TEST_OLD_RELEASE;
    const newInput = process.env.NEW_RELEASE ?? process.env.FABRIC_DRAIN_TEST_NEW_RELEASE;
    const oldRelease = fs.realpathSync(oldInput ?? repo);
    const privateTemp = residentDrainTemp(`resident-drain-cooperative-${mode}-`);
    const { root } = privateTemp;
    // Hosted CI has one fresh build. Its target is a physically separate retained
    // copy of that real build, not dummy JS, hardlinks or a symlinked dist. Main's
    // explicit proof instead supplies TWO independently rebuilt frozen roots.
    const defaultTarget = path.join(root, "target-release");
    if (!newInput) {
      fs.mkdirSync(defaultTarget, { mode: 0o700 });
      fs.cpSync(path.join(oldRelease, "dist"), path.join(defaultTarget, "dist"), { recursive: true });
      fs.copyFileSync(path.join(oldRelease, "package.json"), path.join(defaultTarget, "package.json"));
    }
    const targetRelease = fs.realpathSync(newInput ?? defaultTarget);
    expect(targetRelease).not.toBe(oldRelease);
    const node = findExecutable("node"); expect(node, "native Node runtime required").toBeTruthy();
    const launcherPath = path.join(oldRelease, "dist/residency/launcher.js");
    const hostEntry = path.join(oldRelease, "dist/residency/pi-entry.js");
    const realWorker = path.join(oldRelease, "dist/worker.js");
    const command = path.join(oldRelease, "bin/fabric-resident-drain");
    const piBinary = fs.realpathSync(path.join(oldRelease, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    for (const release of [oldRelease, targetRelease]) {
      for (const entry of ["dist/index.js", "dist/worker.js", "dist/worker-protocol.json", "dist/residency/launcher.js",
        "dist/residency/pi-entry.js", "dist/residency/actor-client.js", "dist/resident-drain-cli.js"]) {
        expect(fs.statSync(path.join(release, entry)).isFile(), `real compiled branch artifact required: ${release}/${entry}`).toBe(true);
      }
    }
    const aWorker = fs.statSync(realWorker); const bWorker = fs.statSync(path.join(targetRelease, "dist/worker.js"));
    expect(`${aWorker.dev}:${aWorker.ino}`, "B must be a separate retained artifact, not A hard-linked/aliased").not.toBe(`${bWorker.dev}:${bWorker.ino}`);
    // Preserve every inherited preload, including an explicitly supplied recorder.
    // Only main's guard ceiling is narrowed to this fixture's mutable work root;
    // its private HOME is a sibling, never part of the permitted deletion tree.
    const inheritedOptions = process.env.NODE_OPTIONS ?? "";
    const recording = process.env.FABRIC_DRAIN_RECORDING_PATH ??
      inheritedOptions.match(/--import(?:=|\s+)([^\s]*record-removals\.mjs)/)?.[1];
    if (recording) expect(inheritedOptions, "preserve explicitly supplied local recording preload").toContain(recording);
    const recordingFile = recording?.startsWith("file:") ? fileURLToPath(recording) : recording;
    const recordingLog = recordingFile ? path.join(path.dirname(recordingFile), "recorded-removals.jsonl") : undefined;
    const owned = launchLog(root);
    const proofOut = process.env.FABRIC_DRAIN_PROOF_OUT;
    const evidenceDir = proofOut ? path.resolve(`${proofOut}.artifacts`, mode) : undefined;
    if (evidenceDir) fs.mkdirSync(evidenceDir, { recursive: true, mode: 0o700 });
    const preserve = (source: string, name: string) => {
      if (evidenceDir && fs.existsSync(source)) fs.copyFileSync(source, path.join(evidenceDir, name));
    };
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir, { mode: 0o700 });
    const wrongRelease = path.join(root, "wrong-release"); fs.mkdirSync(wrongRelease, { mode: 0o700 });
    const meshRoot = path.join(root, "mesh");
    const identity = { id: `session:cooperative-${mode}`, name: "Main", kind: "main" as const, sessionId: `cooperative-${mode}` };
    const workerPath = mode === "legacy-aliased" ? path.join(root, "old-worker-alias.js") : realWorker;
    if (mode === "legacy-aliased") fs.symlinkSync(realWorker, workerPath);
    const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot, actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
      residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: true, nice: 19,
        sessionExport: false, timeoutMs: 180_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, residentIdleExitMs: 180_000 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath, fabricExtensionPath: path.join(oldRelease, "dist/index.js"), piBinary,
      claudeBinary: "missing-claude", vedaBinary: "missing-veda",
      piModels: { available: [{ provider: "offline-fixture", id: "reply" }], aliases: {}, defaultModel: "offline-fixture/reply" },
      watchdog: { enabled: false } };
    fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
    const configPath = path.join(config.residencyRoot, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(config), { mode: 0o600 });
    const setupFile = path.join(root, "main-setup.json"); fs.writeFileSync(setupFile, JSON.stringify({ config, identity, oldRelease, targetRelease }));
    const ownerFile = path.join(config.residencyRoot, "owner.json");
    const cursorFile = path.join(config.residencyRoot, "actor-mesh-cursor.json.project");
    const snapshots: ResidentDrainReceipt[] = [];
    const capture = () => {
      for (const name of fs.readdirSync(config.residencyRoot).filter(name => /^drain-[0-9a-f-]+\.json$/.test(name))) {
        try {
          const value = readJson<ResidentDrainReceipt>(path.join(config.residencyRoot, name));
          if (!snapshots.some(snapshot => JSON.stringify(snapshot) === JSON.stringify(value))) snapshots.push(value);
        } catch { /* A rename commit will produce another observation edge. */ }
      }
    };
    const observation = observeFixture(root, capture); const until = observation.until;
    const registry = new ActorRegistryStore(config.actorRoot);
    const env: NodeJS.ProcessEnv = Object.fromEntries(["PATH", "LANG", "LC_ALL", "GITHUB_ACTIONS", "CI"].flatMap(key =>
      process.env[key] === undefined ? [] : [[key, process.env[key]]]));
    const childIsolation = privateTemp.childEnvironment(owned.env.NODE_OPTIONS!);
    Object.assign(env, owned.env, childIsolation, { PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir,
      FABRIC_DRAIN_RECORD_ROOT: root, PI_FABRIC_MESH_ROOT: meshRoot, PI_FABRIC_PROJECT_ROOT: root,
      PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
      PI_FABRIC_PI_BINARY: piBinary, PI_FABRIC_NODE_BINARY: node });
    // No report-mode spoofing or dropped guard/recording preload. The helper
    // verifies the original physical ceiling before making it strictly narrower.
    expect(env.GITHUB_ACTIONS).toBe(process.env.GITHUB_ACTIONS);
    expect(env.NODE_OPTIONS).toContain("test-deletion-guard.mjs");
    if (recording) expect(env.NODE_OPTIONS).toContain(recording);
    expect(env.NODE_OPTIONS).toContain("launch-preload.mjs");
    const requests: Array<{ index: number; at: number; method: string | undefined; url: string | undefined; model: string; messages: unknown; tools: unknown }> = [];
    const responses = new Map<number, http.ServerResponse>(); const pending = new Set<http.ServerResponse>();
    const providerErrors: string[] = [];
    const reply = (index: number) => {
      const response = responses.get(index); expect(response, `real inference response ${index}`).toBeDefined();
      expect(response!.destroyed || response!.writableEnded, `inference ${index} must not have been aborted`).toBe(false);
      response!.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response!.write(`data: ${JSON.stringify({
        id: `cooperative-${index}`, object: "chat.completion.chunk", created: 1, model: "reply",
        choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      chunk({ role: "assistant", content: `NATIVE_REPLY_${index}` }); chunk({}, "stop"); response!.end("data: [DONE]\n\n");
    };
    const server = http.createServer((request, response) => {
      pending.add(response); response.once("close", () => { pending.delete(response); observation.wake(); });
      let body = ""; request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        try {
          const payload = JSON.parse(body) as { model: string; messages: unknown; tools?: unknown };
          const index = requests.length + 1; responses.set(index, response);
          requests.push({ index, at: Date.now(), method: request.method, url: request.url, model: payload.model,
            messages: payload.messages, tools: payload.tools });
          fs.appendFileSync(path.join(root, "provider-requests.jsonl"), JSON.stringify(requests.at(-1)) + "\n"); observation.wake();
        } catch (error) { providerErrors.push(String(error)); response.writeHead(500); response.end(String(error)); }
      });
    });
    let launcher: ChildProcess | undefined; let mainChild: ChildProcess | undefined;
    let launcherLifetime: ReturnType<typeof watchResidentChild> | undefined;
    let mainLifetime: ReturnType<typeof watchResidentChild> | undefined;
    const cliLifetimes: Array<ReturnType<typeof watchResidentChild>> = [];
    let stderr = ""; const commands: CliResult[] = [];
    const traces = (): Array<Record<string, unknown>> => {
      try { return fs.readFileSync(path.join(config.residencyRoot, "launcher.log"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)); }
      catch { return []; }
    };
    const drainFiles = () => fs.readdirSync(config.residencyRoot).filter(name => /^drain-[0-9a-f-]+\.json$/.test(name)).sort();
    let privateOwner: ResidentHostOwner | undefined;
    const fingerprint = (token: string) => `sha256:${createHash("sha256").update(token).digest("hex").slice(0, 16)}`;
    const invoke = async (expectedRelease: string): Promise<CliResult> => {
      const child = spawn(node!, [command, "--resident", config.residencyRoot, "--mesh-root", meshRoot,
        "--expected-release", expectedRelease, "--target-release", targetRelease], { cwd: root, env, stdio: ["ignore", "pipe", "pipe"] });
      const lifetime = watchResidentChild(child); cliLifetimes.push(lifetime);
      let stdout = ""; let commandStderr = "";
      child.stdout!.on("data", chunk => { stdout += chunk; }); child.stderr!.on("data", chunk => { commandStderr += chunk; });
      const closed = new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", () => resolve()); });
      const timer = setTimeout(() => { void lifetime.stop().catch(() => undefined); }, 140_000);
      try {
        const exit = await lifetime.exit; await closed;
        expect(exit.signal, `CLI must finish normally, not be killed: ${commandStderr}`).toBeNull(); expect(exit.code).not.toBe(0);
        const lines = stdout.trim().split("\n"); expect(lines).toHaveLength(1);
        const receipt = JSON.parse(lines[0]!) as CliReceipt;
        expect(receipt).toMatchObject({ resident: path.relative(meshRoot, config.residencyRoot),
          expected: path.relative(meshRoot, expectedRelease), target: path.relative(meshRoot, targetRelease) });
        // A valid ../ relative path can contain an absolute-root substring;
        // verify the parsed path values, not a misleading substring exclusion.
        for (const value of [receipt.resident, receipt.expected, receipt.target,
          receipt.observed?.loadedRelease.releaseRoot, receipt.observed?.loadedRelease.fabricExtensionPath]) {
          if (value !== undefined && value !== null) expect(path.isAbsolute(value)).toBe(false);
        }
        if (privateOwner) {
          expect(stdout).not.toContain(privateOwner.token);
          if (privateOwner.handover) expect(stdout).not.toContain(privateOwner.handover.launcher.token);
          expect(receipt.observed).toMatchObject({ tokenFingerprint: fingerprint(privateOwner.token) });
          expect(receipt.observed).not.toHaveProperty("token");
          if (receipt.drain) {
            expect(receipt.drain.owner).toMatchObject({ tokenFingerprint: fingerprint(privateOwner.token) });
            expect(receipt.drain.owner).not.toHaveProperty("token"); expect(receipt.drain.launcher).not.toHaveProperty("token");
            expect(receipt.drain.previous.releaseRoot).toBe(path.relative(meshRoot, oldRelease));
            expect(receipt.drain.target.releaseRoot).toBe(path.relative(meshRoot, targetRelease));
          }
        }
        const result = { receipt, stdout, stderr: commandStderr, exit };
        commands.push(result); fs.writeFileSync(path.join(root, `cli-${commands.length}.json`), JSON.stringify(result, null, 2));
        return result;
      } finally { clearTimeout(timer); }
    };
    try {
      await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
      fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "offline-fixture": {
        baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "offline-fixture-only", api: "openai-completions",
        models: [{ id: "reply", name: "reply", reasoning: false, input: ["text"], contextWindow: 200000, maxTokens: 4096,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
      } } }));
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false,
        compaction: { enabled: false }, retry: { enabled: false } }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
        agents: { notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false }, mesh: { enabled: true, actorPollMs: 20 },
        mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false },
        prewalk: { enabled: false, alwaysRearm: false } }));
      const mainEntry = await compileMain(root, oldRelease);
      mainChild = spawn(node!, [mainEntry, setupFile], { cwd: root, env, stdio: ["ignore", "ignore", "pipe", "ipc"] });
      mainLifetime = watchResidentChild(mainChild); const rpc = mainRpc(mainChild, observation.wake);
      mainChild.stderr!.on("data", chunk => { stderr = `${stderr}\nMain: ${chunk}`.slice(-18000); });
      void mainLifetime.exit.then(observation.wake);
      await until(() => {
        if (mainLifetime!.exited) throw new Error(`Real Main exited before readiness: ${stderr}`);
        return fs.existsSync(path.join(root, "main-ready.json"));
      }, "real Main participant/control readiness");
      const mainReady = readJson<{ main: ResidentMainGeneration }>(path.join(root, "main-ready.json"));
      await until(() => owned.owned().some(record => record.pid === mainChild!.pid), "Main self-recorded birth");
      const mainBirth = owned.owned().find(record => record.pid === mainChild!.pid)!;
      expect(mainReady.main).toMatchObject({ pid: mainBirth.pid, processStartTime: mainBirth.started, releaseRoot: oldRelease,
        rootId: identity.id, sessionId: identity.sessionId });
      expect(mainBirth.argv).toContain(mainEntry); expect(same(mainBirth) && live(mainBirth.pid)).toBe(true);
      launcher = spawn(node!, [launcherPath, "--config", configPath], { cwd: root, env, stdio: ["ignore", "ignore", "pipe"] });
      launcherLifetime = watchResidentChild(launcher); launcher.on("error", observation.wake);
      void launcherLifetime.exit.then(observation.wake);
      launcher.stderr!.on("data", chunk => { stderr = `${stderr}\nLauncher: ${chunk}`.slice(-18000); });
      await until(() => fs.existsSync(ownerFile) && fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json")), "native resident readiness");
      const ownerBytes = fs.readFileSync(ownerFile, "utf8"); const owner = JSON.parse(ownerBytes) as ResidentHostOwner;
      privateOwner = owner;
      await until(() => owned.owned().some(record => record.pid === owner.pid) && owned.owned().some(record => record.pid === launcher!.pid), "resident and launcher birth receipts");
      const ownerBirth = owned.owned().find(record => record.pid === owner.pid)!;
      const launcherBirth = owned.owned().find(record => record.pid === launcher!.pid)!;
      expect(ownerBirth.argv).toContain(piBinary); expect(ownerBirth.argv).toContain(hostEntry);
      expect(owner.processStartTime).toBe(ownerBirth.started); expect(launcherBirth.argv).toContain(launcherPath);
      if (mode === "legacy-aliased") {
        expect(owner.releaseRoot).toBeUndefined(); expect(owner.handover).toBeUndefined(); expect(owner.drain).toBeUndefined();
      } else {
        expect(owner).toMatchObject({ releaseRoot: oldRelease, drain: { abi: RESIDENT_DRAIN_ABI },
          handover: { launcher: { pid: launcherBirth.pid, processStartTime: launcherBirth.started, entry: hostEntry } } });
        expect(owner.configDigest).toMatch(/^[0-9a-f]{64}$/);
      }
      preserve(ownerFile, "owner-before.json"); preserve(configPath, "config.json");
      const actor = await rpc<FabricActorInfo>("create", { name: `cooperative-${mode}`, instructions: "Reply briefly to each input.",
        residency: "durable", model: "offline-fixture/reply", transport: "process", tools: [], extensions: false,
        delivery: "mailbox", responseMode: "text", coalesce: false, topics: ["native.drain.concurrent"] });
      const history = (): FabricActorMessage[] => {
        const row = registry.records().find(record => record.id === actor.id);
        return row ? registry.messages(row) as FabricActorMessage[] : [];
      };
      const queueFiles = () => {
        const directory = path.join(config.actorRoot, actor.id);
        return fs.readdirSync(directory).filter(name => /^queue-.*\.json$/.test(name)).sort().map(name => path.join(directory, name));
      };
      const queueItems = () => queueFiles().flatMap(file => readJson<QueueSnapshot>(file).items);
      const first = await rpc<Accepted>("followUp", { id: actor.id, message: "NATIVE_FIRST_INPUT active until natural SSE completion" });
      expect(first.acknowledged).toBe(true); expect(first.messageId).toBeTruthy();
      await until(() => requests.length === 1 && responses.has(1), "real worker -> native Pi HTTP inference");
      expect(requests[0]).toMatchObject({ method: "POST", model: "reply" }); expect(requests[0]!.tools ?? []).toEqual([]);
      expect(JSON.stringify(requests[0]!.messages)).toContain("NATIVE_FIRST_INPUT");
      const second = await rpc<Accepted>("followUp", { id: actor.id, message: "NATIVE_SECOND_INPUT accepted before PREPARE" });
      expect(second.acknowledged).toBe(true); expect(second.messageId).toBeTruthy(); expect(second.messageId).not.toBe(first.messageId);
      await until(() => queueItems().some(item => item.id === second.messageId), "accepted queue durable before hold");
      const queueBefore = queueFiles().map(file => ({ file, bytes: fs.readFileSync(file, "utf8") }));
      const accepted = queueItems();
      expect(accepted.filter(item => item.id === first.messageId)).toEqual([expect.objectContaining({
        attempts: 0, launchEvidenceVersion: 1, executionStarted: true })]);
      expect(accepted.filter(item => item.id === second.messageId)).toEqual([expect.objectContaining({
        attempts: 0, launchEvidenceVersion: 1, executionStarted: false })]);
      fs.writeFileSync(path.join(root, "queue-before.json"), JSON.stringify(queueBefore, null, 2));
      const spawnedBefore = traces().filter(row => row.event === "child-spawned"); expect(spawnedBefore).toHaveLength(1);
      const assertA = () => {
        expect(fs.readFileSync(ownerFile, "utf8")).toBe(ownerBytes);
        expect(same(ownerBirth) && live(ownerBirth.pid)).toBe(true); expect(same(launcherBirth) && live(launcherBirth.pid)).toBe(true);
        expect(launcherLifetime!.exited).toBe(false); expect(traces().filter(row => row.event === "child-spawned")).toEqual(spawnedBefore);
        expect(owned.owned().filter(record => record.argv.some(arg => arg.startsWith(path.join(targetRelease, "dist") + path.sep)))).toEqual([]);
        expect(fs.existsSync(path.join(config.residencyRoot, "handover.json"))).toBe(false);
        expect(fs.existsSync(path.join(config.residencyRoot, "watchdog-custody.json"))).toBe(false);
      };
      const drainBefore = drainFiles(); const mainBytes = fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8");
      const mismatch = await invoke(wrongRelease);
      expect(mismatch.receipt).toMatchObject({ ok: false, phase: "preflight", outcome: "refused", code: "EXPECTED_RELEASE_MISMATCH",
        unchanged: true, transitionAttempted: false });
      assertA(); expect(drainFiles()).toEqual(drainBefore); expect(requests).toHaveLength(1);
      expect(queueFiles().map(file => ({ file, bytes: fs.readFileSync(file, "utf8") }))).toEqual(queueBefore);
      expect(fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8")).toBe(mainBytes);
      let terminal: ResidentDrainReceipt | undefined; let concurrent: Published | undefined;
      let held: ResidentDrainReceipt | undefined; let direct: ResidentCommandResponse | undefined;
      if (mode === "legacy-aliased" || mode === "lost-main") {
        if (mode === "lost-main") {
          await rpc("exit"); const exited = await mainLifetime.exit;
          expect(exited).toEqual({ code: 0, signal: null }); expect(same(mainBirth) && live(mainBirth.pid)).toBe(false);
        }
        const refusal = await invoke(oldRelease);
        expect(refusal.receipt).toMatchObject({ ok: false, phase: "preflight", outcome: "refused", unchanged: true, transitionAttempted: false });
        if (mode === "legacy-aliased") expect(refusal.receipt.code).toBe("MISSING_LAUNCH_ATTESTATION");
        else expect(`${refusal.receipt.code} ${refusal.receipt.reason}`).toMatch(/main|generation|caller/i);
        expect(drainFiles()).toEqual(drainBefore); expect(requests).toHaveLength(1); assertA();
        expect(queueFiles().map(file => ({ file, bytes: fs.readFileSync(file, "utf8") }))).toEqual(queueBefore);
        expect(fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8")).toBe(mainBytes);
        reply(1);
      } else {
        // Observe the held boundary with active HTTP deliberately unfinished.
        const cliPending = mode === "checkpoint-concurrent" ? invoke(oldRelease) : undefined;
        // Attach immediately, so a failed native CLI cannot become unhandled while
        // the filesystem waiter diagnoses the retained fixture.
        void cliPending?.catch(() => observation.wake());
        if (mode === "timeout") {
          direct = await rpc<ResidentCommandResponse>("drain", { timeoutMs: 5_000 });
          expect(direct.ok).toBe(true); expect(direct.drain).toBeDefined(); held = direct.drain;
          expect(held!.id).not.toBe(direct.requestId);
        }
        await until(() => drainFiles().some(file => readJson<ResidentDrainReceipt>(path.join(config.residencyRoot, file)).phase === "held"), "resident-generated held receipt");
        held ??= readJson<ResidentDrainReceipt>(path.join(config.residencyRoot, drainFiles().find(file => readJson<ResidentDrainReceipt>(path.join(config.residencyRoot, file)).phase === "held")!));
        expect(held).toMatchObject({ abi: RESIDENT_DRAIN_ABI, rootId: identity.id, phase: "held", outcome: "held", admissionHeld: true,
          main: mainReady.main, owner: { pid: ownerBirth.pid, processStartTime: ownerBirth.started, token: owner.token, hostId: owner.hostId },
          launcher: owner.handover!.launcher, previous: { releaseRoot: oldRelease }, target: { releaseRoot: targetRelease } });
        expect(held!.id).toMatch(/^[0-9a-f-]{36}$/); expect(held!.deadlineAt).toBeGreaterThan(held!.at);
        assertA(); expect(requests).toHaveLength(1); expect(queueItems().filter(item => item.id === second.messageId)).toEqual(accepted.filter(item => item.id === second.messageId));
        preserve(residentDrainReceiptPath(config.residencyRoot, held!.id), "drain-held.json");
        const receiptFile = residentDrainReceiptPath(config.residencyRoot, held!.id);
        if (mode === "checkpoint-concurrent") {
          await until(() => fs.existsSync(cursorFile), "project actor cursor before concurrent publication");
          const cursorBefore = readJson<Cursor>(cursorFile);
          concurrent = await rpc<Published>("publish", { id: actor.id, message: "NATIVE_CONCURRENT_INPUT published behind held cursor" });
          expect(concurrent.id).toBeTruthy(); expect(concurrent.sequence).toBeGreaterThan(cursorBefore.last?.sequence ?? 0);
          // Producer commit is real and durable, but it is not accepted by this
          // resident yet. Neither filtering nor an optimistic cursor may consume it.
          expect(readJson<Cursor>(cursorFile)).toEqual(cursorBefore);
          expect(queueItems().some(item => item.payload?.id === concurrent!.id)).toBe(false);
          expect(history().some(message => (message.data as { id?: string } | undefined)?.id === concurrent!.id)).toBe(false); expect(requests).toHaveLength(1);
          fs.writeFileSync(path.join(root, "held-producer.json"), JSON.stringify({ held, concurrent, cursorBefore, queueItems: queueItems() }, null, 2));
          reply(1); // Active work settles naturally; only now may checkpoint finish.
        }
        await until(() => readJson<ResidentDrainReceipt>(receiptFile).admissionHeld === false &&
          ["containment-unavailable", "resumed", "refused", "needs-operator"].includes(readJson<ResidentDrainReceipt>(receiptFile).outcome), "terminal resume of the same A", 30_000);
        terminal = readJson<ResidentDrainReceipt>(receiptFile); capture();
        expect(terminal).toMatchObject({ abi: RESIDENT_DRAIN_ABI, id: held!.id, rootId: identity.id,
          owner: held!.owner, launcher: held!.launcher, main: mainReady.main, admissionHeld: false });
        assertA(); expect(fs.readFileSync(path.join(config.residencyRoot, "main-generation.json"), "utf8")).toBe(mainBytes);
        const decisions = fs.readdirSync(path.join(config.residencyRoot, "decisions")).filter(file => file.endsWith(".json"))
          .map(file => readJson<{ state: string; id?: string; requestId: string; operation?: string; ownerHostId?: string }>(path.join(config.residencyRoot, "decisions", file)))
          .filter(value => value.operation === "releaseDrain");
        expect(decisions).toEqual([expect.objectContaining({ state: "committed", id: held!.id, ownerHostId: owner.hostId })]);
        expect(decisions[0]!.requestId).not.toBe(held!.id);
        if (mode === "checkpoint-concurrent") {
          expect(terminal).toMatchObject({ phase: "checkpointed", outcome: "containment-unavailable", checkpointReadback: true });
          expect(terminal.checkpoint).toBeDefined();
          const checkpointText = JSON.stringify(terminal.checkpoint);
          expect(checkpointText).toContain(second.messageId!);
          expect(checkpointText).toContain("attempts"); expect(checkpointText).toContain("executionStarted");
          const scopes = (terminal.checkpoint as { actors: { project: { queues: Array<{ items: QueueItem[] }>; cursor: Cursor };
            session: { queues: unknown[]; cursor: Cursor } } }).actors;
          expect(scopes.session.cursor.format).toBe(1); expect(scopes.project.cursor.format).toBe(1);
          expect(scopes.project.cursor.last?.sequence ?? 0).toBeLessThan(concurrent!.sequence);
          expect(scopes.project.queues.flatMap(queue => queue.items).some(item => item.payload?.id === concurrent!.id)).toBe(false);
          expect(scopes.project.queues.flatMap(queue => queue.items).filter(item => item.id === second.messageId)).toEqual([
            expect.objectContaining({ id: second.messageId, attempts: 0, launchEvidenceVersion: 1, executionStarted: false })]);
          // Receipt evidence must include the exact queued ID's attempt fields,
          // not just a UI count. Its durable bytes are independently preserved.
          const evidenceItems = (value: unknown): QueueItem[] => {
            if (Array.isArray(value)) return value.flatMap(evidenceItems);
            if (!value || typeof value !== "object") return [];
            const item = value as Record<string, unknown>;
            return [...(item.id === second.messageId && "attempts" in item ? [item as QueueItem] : []), ...Object.values(item).flatMap(evidenceItems)];
          };
          expect(evidenceItems(terminal.checkpoint)).toContainEqual(expect.objectContaining({ id: second.messageId,
            attempts: 0, launchEvidenceVersion: 1, executionStarted: false }));
          const result = await cliPending!;
          expect(result.receipt).toMatchObject({ ok: false, outcome: "containment-unavailable", unchanged: false, transitionAttempted: true });
        } else {
          expect(terminal.outcome).toBe("resumed"); expect(`${terminal.code} ${terminal.reason}`).toMatch(/timeout|timed out|deadline/i);
          expect(requests).toHaveLength(1); expect(responses.get(1)!.destroyed || responses.get(1)!.writableEnded).toBe(false);
          expect(history().filter(message => message.direction === "out")).toEqual([]);
          expect(queueItems().filter(item => item.id === second.messageId)).toEqual(accepted.filter(item => item.id === second.messageId));
          reply(1); // Timeout did not abort either HTTP inference or its native worker.
        }
        preserve(receiptFile, "drain-terminal.json");
      }
      await until(() => requests.length === 2 && history().some(message => message.direction === "out" && message.text === "NATIVE_REPLY_1"), "queued work starts on resumed A after natural first completion");
      const firstOutput = history().find(message => message.direction === "out" && message.text === "NATIVE_REPLY_1")!;
      expect(firstOutput.createdAt).toBeLessThanOrEqual(requests[1]!.at);
      expect(JSON.stringify(requests[1]!.messages)).toContain("NATIVE_SECOND_INPUT");
      const activeSecond = queueItems().filter(item => item.id === second.messageId);
      expect(activeSecond).toEqual([expect.objectContaining({ attempts: 0, launchEvidenceVersion: 1, executionStarted: true })]);
      reply(2);
      if (concurrent) {
        await until(() => requests.length === 3 && history().some(message => message.direction === "out" && message.text === "NATIVE_REPLY_2"), "held concurrent producer delivered once after queued input");
        expect(JSON.stringify(requests[2]!.messages)).toContain("NATIVE_CONCURRENT_INPUT");
        reply(3);
      }
      const expectedCount = concurrent ? 3 : 2;
      await until(() => history().filter(message => message.direction === "out").length === expectedCount &&
        !queueItems().some(item => [first.messageId, second.messageId].includes(item.id) ||
          (concurrent !== undefined && item.payload?.id === concurrent.id)), "all accepted native work completes once");
      const messages = history();
      for (const [id, index] of [[first.messageId, 1], [second.messageId, 2]] as Array<[string, number]>) {
        expect(messages.filter(message => message.direction === "in" && message.id === id)).toHaveLength(1);
        expect(messages.filter(message => message.direction === "out" && message.text === `NATIVE_REPLY_${index}`)).toHaveLength(1);
      }
      if (concurrent) {
        // Mesh event IDs live in the accepted item's payload; history IDs are
        // actor activation IDs. Bind the exact producer event, not an unrelated UUID.
        expect(messages.filter(message => message.direction === "in" && message.source === "mesh:native.drain.concurrent" &&
          (message.data as { id?: string } | undefined)?.id === concurrent!.id)).toHaveLength(1);
        expect(messages.filter(message => message.direction === "out" && message.text === "NATIVE_REPLY_3")).toHaveLength(1);
      }
      expect(messages.some(message => message.error !== undefined || message.stale === true)).toBe(false);
      expect(requests).toHaveLength(expectedCount); expect(providerErrors).toEqual([]); assertA();
      if (mode !== "lost-main") {
        const status = await rpc<FabricActorInfo>("status", { id: actor.id });
        expect(["idle", "dormant"]).toContain(status.status); expect(status.queued).toBe(0);
        expect(status.inFlightRun).toBeUndefined(); expect(status.lastError).toBeUndefined();
      }
      const launches = owned.owned();
      expect(launches.filter(record => record.argv[0] === piBinary)).toHaveLength(expectedCount + 1);
      expect(launches.filter(record => record.argv[0] === realWorker || record.argv[0] === workerPath)).toHaveLength(expectedCount);
      expect(launches.every(record => [mainEntry, launcherPath, piBinary, realWorker, workerPath, command].includes(record.argv[0] ?? ""))).toBe(true);
      for (const record of launches) expect(record.started).toMatch(/^\d+$/);
      const target: ResidentLaunchSpec | undefined = terminal?.target;
      const proof = { mode, root, oldRelease, targetRelease, command, nativePi: piBinary, realWorker,
        main: mainReady.main, mainBirth, owner: ownerBirth, launcher: launcherBirth, ownerToken: owner.token,
        actorId: actor.id, firstAcceptedId: first.messageId, secondAcceptedId: second.messageId, concurrent,
        queueBefore: accepted, held, terminal, direct, target, snapshots, commands, providerRequests: requests,
        history: messages, unchangedOwner: true, targetLauncherSpawns: 0,
        containment: "unavailable; no whole-attempt-exit claim", nativeDeliveryCount: expectedCount, launches };
      fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(proof, null, 2));
      fs.writeFileSync(path.join(root, "actor-history.json"), JSON.stringify(messages, null, 2));
      fs.writeFileSync(path.join(root, "drain-snapshots.json"), JSON.stringify(snapshots, null, 2));
      preserve(ownerFile, "owner-after.json"); preserve(path.join(config.actorRoot, "actors.json"), "actor-registry.json");
    } catch (error) {
      let childLog = "";
      try { childLog = fs.readFileSync(path.join(config.residencyRoot, "child-stderr.log"), "utf8").slice(-18000); } catch { /* not started */ }
      fs.writeFileSync(path.join(root, "failure.json"), JSON.stringify({ error: String(error), stderr, childLog, traces: traces(), requests, commands, snapshots }, null, 2));
      throw new Error(`${error instanceof Error ? error.stack : error}\nretained fixture: ${root}\n${stderr}\nchild: ${childLog}`);
    } finally {
      for (const response of pending) if (!response.writableEnded) response.end();
      try {
        await Promise.all(cliLifetimes.filter(lifetime => !lifetime.exited).map(lifetime => lifetime.stop()));
        if (launcherLifetime && !launcherLifetime.exited) await launcherLifetime.stop();
        if (mainLifetime && !mainLifetime.exited) await mainLifetime.stop();
      } finally {
        // Cleanup is deliberately NOT a containment receipt. Only self-recorded,
        // fresh exact fixture births may be signalled. No fleet scan or name kill.
        for (const record of owned.owned()) if (same(record) && live(record.pid)) {
          try { process.kill(record.pid, "SIGKILL"); } catch { /* exact birth already exited */ }
        }
        await Promise.all([launcherLifetime?.exit, mainLifetime?.exit]); registry.releaseReadCache();
        await new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); });
        // One finite scheduling grace for residual exact-birth cleanup, then one
        // read. Never a polling loop and never authority to start B.
        await new Promise<void>(resolve => setTimeout(resolve, 1_000));
        const liveFixturePids = owned.owned().filter(record => same(record) && live(record.pid)).map(record => record.pid);
        const recordedRemovals: Array<{ pid: number; raw?: string; actual?: string; deleted: boolean }> = recordingLog && fs.existsSync(recordingLog)
          ? fs.readFileSync(recordingLog, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line))
            .filter((row: { pid: number; raw?: string; actual?: string }) => owned.owned().some(record => record.pid === row.pid) &&
              [row.raw, row.actual].some(file => file === root || file?.startsWith(root + path.sep))) : [];
        if (recording) {
          if (fs.existsSync(path.join(root, "proof.json"))) {
            expect(recordedRemovals.length, "local native calls must be recorded, not silently execute deletions").toBeGreaterThan(0);
          }
          expect(recordedRemovals.every(row => row.deleted === false)).toBe(true);
          fs.writeFileSync(path.join(root, "recorded-removals.json"), JSON.stringify(recordedRemovals, null, 2));
        }
        const cleanup = { retained: true, launches: owned.owned(), liveFixturePids, remainingLiveFixtureCount: liveFixturePids.length,
          localRecording: !!recording, recordedRemovals };
        observation.close(); fs.writeFileSync(path.join(root, "cleanup.json"), JSON.stringify(cleanup, null, 2));
        for (const name of ["proof.json", "cleanup.json", "failure.json", "actor-history.json", "drain-snapshots.json", "main-ready.json",
          "main-compile.json", "provider-requests.jsonl", "launches.jsonl", "queue-before.json", "held-producer.json", "recorded-removals.json",
          "deletion-guard-audit.jsonl"]) preserve(path.join(root, name), name);
        commands.forEach((_, index) => preserve(path.join(root, `cli-${index + 1}.json`), `cli-${index + 1}.json`));
        preserve(path.join(config.residencyRoot, "launcher.log"), "launcher.log");
        preserve(path.join(config.residencyRoot, "child-stderr.log"), "child-stderr.log");
        expect(liveFixturePids, "fixture-owned native processes must be quiescent after cleanup").toEqual([]);
        if (mainChild) expect(fs.existsSync(path.join(root, "deletion-guard-audit.jsonl")), "real children must validate and audit the standard guard").toBe(true);
        privateTemp.assertNoOutsideDeletion(); // Hosted reports fail too, not just local refusals.
        if (fs.existsSync(path.join(root, "proof.json"))) {
          const finalProof = { ...readJson<Record<string, unknown>>(path.join(root, "proof.json")), cleanup };
          fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(finalProof, null, 2)); preserve(path.join(root, "proof.json"), "proof.json");
          console.info("resident cooperative native proof", JSON.stringify({ mode, root, oldRelease, targetRelease, proofOut, evidenceDir,
            outcome: ((finalProof as Record<string, unknown>).terminal as ResidentDrainReceipt | undefined)?.outcome,
            code: ((finalProof as Record<string, unknown>).terminal as ResidentDrainReceipt | undefined)?.code,
            targetLauncherSpawns: 0, remainingLiveFixtureCount: liveFixturePids.length }));
          if (proofOut) { fs.mkdirSync(path.dirname(path.resolve(proofOut)), { recursive: true }); fs.appendFileSync(proofOut, JSON.stringify(finalProof) + "\n"); }
        }
        // Intentionally no deletion: roots, artifacts and recorded-removal evidence remain.
      }
    }
  }, 180_000);
});
