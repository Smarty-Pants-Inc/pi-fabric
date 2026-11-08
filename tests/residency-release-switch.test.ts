import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { build } from "esbuild";
import { fileURLToPath } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricActorInfo, FabricActorMessage } from "../src/actors/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { residentProcessAlive } from "../src/residency/process-identity.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { residentDrainReceiptPath, type ResidentDrainReceipt } from "../src/residency/drain.js";
import { residentGenerationExitPath, type ResidentGenerationExit } from "../src/residency/generation-exit.js";
import { handoverPath, type ResidentMainGeneration } from "../src/residency/handover.js";
import { residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { launchLog, same, stopAllOwned } from "./helpers/owned-processes.js";
import { residentDrainTemp } from "./helpers/resident-drain-temp.js";

const repo = path.resolve(fileURLToPath(new URL("../", import.meta.url)));
const readJson = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
type QueueItem = { id: string; payload?: { id?: string }; attempts?: number; executionStarted?: boolean };
type QueueSnapshot = { items: QueueItem[] };
type ProviderRequest = { index: number; model: string; messages: unknown };

// Keep: compiled Main/launcher/host control boundary and accepted queue data
// safety. The sole substitute is inference on a local SSE endpoint.
async function compileMain(root: string) {
  const runtimeRoot = path.join(root, "main-runtime");
  const entry = path.join(runtimeRoot, "dist/fixture/resident-switch-main.mjs");
  fs.mkdirSync(path.dirname(entry), { recursive: true, mode: 0o700 });
  fs.copyFileSync(path.join(repo, "package.json"), path.join(runtimeRoot, "package.json"));
  fs.cpSync(path.join(repo, "dist/native"), path.join(runtimeRoot, "dist/native"), { recursive: true });
  fs.symlinkSync(path.join(repo, "node_modules"), path.join(runtimeRoot, "node_modules"), "dir");
  const nativeDigests = Object.fromEntries(fs.readdirSync(path.join(runtimeRoot, "dist/native")).flatMap(name => {
    const file = path.join(runtimeRoot, "dist/native", name);
    return fs.statSync(file).isFile() && !name.endsWith(".json")
      ? [[name, createHash("sha256").update(fs.readFileSync(file)).digest("hex")]] : [];
  }));
  // Resolve from this checkout, including an immutable git-archive with no .git.
  const sourceRoot = repo;
  const compiled = await build({ absWorkingDir: sourceRoot,
    entryPoints: [path.join(sourceRoot, "tests/fixtures/resident-switch-main.ts")], outfile: entry,
    bundle: true, packages: "external", platform: "node", format: "esm", target: "node24", metafile: true,
    define: { __FABRIC_NATIVE_DIGESTS__: JSON.stringify(JSON.stringify(nativeDigests)) }, logLevel: "silent" });
  fs.writeFileSync(path.join(root, "main-compile.json"), JSON.stringify({ sourceRoot, entry, nativeDigests,
    sha256: createHash("sha256").update(fs.readFileSync(entry)).digest("hex"), inputs: Object.keys(compiled.metafile!.inputs) }, null, 2));
  return entry;
}

function mainRpc(child: ChildProcess, wake: () => void) {
  const pending = new Map<string, { resolve: (value: unknown) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  let ready: { owner: ResidentHostOwner; main: ResidentMainGeneration } | undefined;
  let failure: Error | undefined;
  const fail = (error: Error) => {
    failure = error;
    for (const waiter of pending.values()) { clearTimeout(waiter.timer); waiter.reject(error); }
    pending.clear(); wake();
  };
  child.on("error", fail); child.on("exit", () => fail(new Error("Native release-switch Main exited")));
  child.on("message", (message: { ready?: boolean; owner: ResidentHostOwner; main: ResidentMainGeneration; id?: string; result?: unknown; error?: string }) => {
    if (message.ready) ready = { owner: message.owner, main: message.main };
    const waiter = message.id ? pending.get(message.id) : undefined;
    if (waiter) { pending.delete(message.id!); clearTimeout(waiter.timer); if (message.error) waiter.reject(new Error(message.error)); else waiter.resolve(message.result); }
    wake();
  });
  return { ready: () => ready,
    call: <T>(operation: string, input: unknown = {}): Promise<T> => new Promise((resolve, reject) => {
      if (failure) { reject(failure); return; }
      const id = randomUUID();
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`Native Main RPC deadline: ${operation}`)); }, 60_000);
      pending.set(id, { resolve: value => resolve(value as T), reject, timer });
      child.send({ id, operation, input }, error => { if (error) fail(error); });
    }),
  };
}

function observe(root: string) {
  const listeners = new Set<() => void>();
  let watchError: Error | undefined;
  const wake = () => { for (const listener of [...listeners]) listener(); };
  const watcher = fs.watch(root, { recursive: true, persistent: false }, wake);
  watcher.on("error", error => { watchError = error; wake(); });
  return {
    wake,
    close: () => watcher.close(),
    until(test: () => boolean, what: string, ms = 90_000): Promise<void> {
      return new Promise((resolve, reject) => {
        const finish = (error?: unknown) => {
          clearTimeout(timer); listeners.delete(check);
          if (error) reject(error); else resolve();
        };
        const check = () => {
          try { if (watchError) throw watchError; if (test()) finish(); }
          catch (error) { finish(error); }
        };
        const timer = setTimeout(() => finish(new Error(`Native release-switch deadline: ${what}`)), ms);
        listeners.add(check); check();
      });
    },
  };
}

const drainReceipts = (root: string): ResidentDrainReceipt[] => {
  try {
    return fs.readdirSync(root).filter(name => /^drain-[0-9a-f-]+\.json$/.test(name)).flatMap(name => {
      try { return [readJson<ResidentDrainReceipt>(path.join(root, name))]; }
      catch { return []; }
    });
  } catch { return []; }
};

const processIsLive = (pid: number): boolean => {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return !["Z", "X"].includes(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]!);
  } catch { return false; }
};

describe.skipIf(process.platform !== "linux" || !fs.existsSync(path.join(repo, "dist/residency/launcher.js")))
  ("Main startup switches a same-root native resident to its release", () => {
    it("drains A after natural completion, starts B, and delivers the committed mesh wake once", async () => {
      const privateTemp = residentDrainTemp("resident-release-switch-");
      const { root } = privateTemp;
      const owned = launchLog(root);
      const node = findExecutable("node");
      expect(node, "native Node runtime required").toBeTruthy();
      const targetRelease = path.join(root, "release-b");
      fs.mkdirSync(targetRelease, { mode: 0o700 });
      fs.cpSync(path.join(repo, "dist"), path.join(targetRelease, "dist"), { recursive: true });
      fs.copyFileSync(path.join(repo, "package.json"), path.join(targetRelease, "package.json"));
      fs.symlinkSync(path.join(repo, "node_modules"), path.join(targetRelease, "node_modules"), "dir");
      const piBinary = path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
      expect(fs.statSync(piBinary).isFile()).toBe(true);
      for (const release of [repo, targetRelease]) {
        for (const artifact of ["dist/index.js", "dist/worker.js", "dist/residency/launcher.js", "dist/residency/pi-entry.js"])
          expect(fs.statSync(path.join(release, artifact)).isFile(), `native release artifact required: ${artifact}`).toBe(true);
      }
      const aWorker = fs.statSync(path.join(repo, "dist/worker.js"));
      const bWorker = fs.statSync(path.join(targetRelease, "dist/worker.js"));
      expect(`${aWorker.dev}:${aWorker.ino}`, "B must be a physically separate build copy").not.toBe(`${bWorker.dev}:${bWorker.ino}`);

      const agentDir = path.join(root, "agent");
      fs.mkdirSync(agentDir, { mode: 0o700 });
      const meshRoot = path.join(root, "mesh");
      const identity = { id: "session:release-switch", name: "Main", kind: "main" as const, sessionId: "release-switch" };
      const config: ResidentHostConfig = {
        format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root, meshRoot,
        actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
        residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
        agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: true,
          nice: 19, sessionExport: false, timeoutMs: 180_000 },
        mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, residentIdleExitMs: 180_000 },
        retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.join(repo, "dist/worker.js"),
        fabricExtensionPath: path.join(repo, "dist/index.js"), piBinary,
        claudeBinary: "missing-claude", vedaBinary: "missing-veda", watchdog: { enabled: false },
        piModels: { available: [{ provider: "offline-fixture", id: "reply" }], aliases: {}, defaultModel: "offline-fixture/reply" },
      };
      fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });

      const isolation = { ...owned.env, ...privateTemp.childEnvironment(owned.env.NODE_OPTIONS!) };
      Object.assign(isolation, {
        PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_MESH_ROOT: meshRoot,
        PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, "runs"),
        PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_PI_BINARY: piBinary,
        PI_FABRIC_NODE_BINARY: node,
      });
      for (const [name, value] of Object.entries(isolation)) vi.stubEnv(name, value);
      expect(process.env.NODE_OPTIONS).toContain("test-deletion-guard.mjs");
      expect(process.env.NODE_OPTIONS).toContain("launch-preload.mjs");

      const serverRequests: ProviderRequest[] = [];
      const responses = new Map<number, http.ServerResponse>();
      const openResponses = new Set<http.ServerResponse>();
      const reply = (index: number): void => {
        const response = responses.get(index);
        expect(response, `native inference response ${index}`).toBeDefined();
        expect(response!.destroyed || response!.writableEnded, "accepted inference must remain available for natural completion").toBe(false);
        response!.writeHead(200, { "Content-Type": "text/event-stream" });
        const chunk = (delta: unknown, finish_reason: string | null = null) => response!.write(`data: ${JSON.stringify({
          id: `resident-switch-${index}`, object: "chat.completion.chunk", created: 1, model: "reply",
          choices: [{ index: 0, delta, finish_reason }],
        })}\n\n`);
        chunk({ role: "assistant", content: `SWITCH_REPLY_${index}` });
        chunk({}, "stop");
        response!.end("data: [DONE]\n\n");
      };
      let observer: ReturnType<typeof observe> | undefined;
      // The call-through observer lives INSIDE compiled Main, sharing the
      // bundled ResidentActorClient class with its lazy lifecycle import.
      const drainReplies = (): ResidentDrainReceipt[] => {
        const file = path.join(root, "main-drain-replies.jsonl");
        if (!fs.existsSync(file)) return [];
        return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).flatMap(line => {
          const row = JSON.parse(line) as { response: { drain?: ResidentDrainReceipt } };
          return row.response.drain ? [row.response.drain] : [];
        });
      };
      const server = http.createServer((request, response) => {
        openResponses.add(response);
        response.once("close", () => { openResponses.delete(response); observer?.wake(); });
        let body = "";
        request.on("data", chunk => { body += chunk; });
        request.on("end", () => {
          const payload = JSON.parse(body) as { model: string; messages: unknown };
          const index = serverRequests.length + 1;
          serverRequests.push({ index, model: payload.model, messages: payload.messages });
          responses.set(index, response);
          fs.appendFileSync(path.join(root, "provider-requests.jsonl"), JSON.stringify(serverRequests.at(-1)) + "\n");
          observer?.wake();
        });
      });
      const registries = { project: new ActorRegistryStore(config.actorRoot), session: new ActorRegistryStore(config.sessionActorRoot!) };
      const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
      let mainChild: ChildProcess | undefined;
      let mainLifetime: ReturnType<typeof watchResidentChild> | undefined;
      let rpc: ReturnType<typeof mainRpc> | undefined;
      let mainStderr = "";
      let serverListening = false;
      let actor: FabricActorInfo | undefined;
      let ownerA: ResidentHostOwner | undefined;
      try {
        await new Promise<void>((resolve, reject) => {
          server.once("error", reject);
          server.listen(0, "127.0.0.1", () => { server.off("error", reject); serverListening = true; resolve(); });
        });
        fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { "offline-fixture": {
          baseUrl: `http://127.0.0.1:${(server.address() as { port: number }).port}/v1`, apiKey: "fixture-only",
          api: "openai-completions", models: [{ id: "reply", name: "reply", reasoning: false, input: ["text"],
            contextWindow: 200000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
        } } }));
        fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ enableInstallTelemetry: false,
          compaction: { enabled: false }, retry: { enabled: false } }));
        fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
          agents: { notifyOnComplete: false, retainRuns: true, nice: 19, sessionExport: false },
          mesh: { enabled: true, actorPollMs: 20 }, mcp: { enabled: false }, memory: { enabled: false },
          jev: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
        observer = observe(root);

        const setup = path.join(root, "main-setup.json");
        fs.writeFileSync(setup, JSON.stringify({ config, identity, oldRelease: repo, targetRelease }));
        const mainEntry = await compileMain(root);
        mainChild = spawn(node!, [mainEntry, setup], { cwd: root, env: process.env, stdio: ["ignore", "ignore", "pipe", "ipc"] });
        mainLifetime = watchResidentChild(mainChild);
        rpc = mainRpc(mainChild, observer.wake);
        mainChild.stderr!.on("data", chunk => {
          mainStderr += String(chunk); fs.appendFileSync(path.join(root, "main-stderr.log"), chunk);
        });
        await observer.until(() => {
          if (mainLifetime!.exited) throw new Error(`Compiled Main startup failed: ${mainStderr}`);
          return !!rpc!.ready();
        }, "compiled native Main launches A and records its authority");
        ownerA = rpc.ready()!.owner;
        const mainBirth = owned.owned().find(row => row.pid === mainChild!.pid);
        expect(mainBirth, "compiled Main must self-record its native birth").toBeDefined();
        expect(mainBirth!.argv[0]).toBe(mainEntry);
        expect(rpc.ready()!.main).toMatchObject({ pid: mainChild.pid, processStartTime: mainBirth!.started, releaseRoot: repo });
        expect(readJson<ResidentMainGeneration>(path.join(config.residencyRoot, "main-generation.json"))).toEqual(rpc.ready()!.main);
        expect(ownerA).toMatchObject({ releaseRoot: repo, drain: { abi: "fabric-resident-drain-1" } });
        const ownerBytes = fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8");
        const ownerBirth = owned.owned().find(row => row.pid === ownerA!.pid);
        expect(ownerBirth, "A host must record its own native birth").toBeDefined();
        expect(ownerBirth!.started).toBe(ownerA.processStartTime);

        // A fresh same-release Main startup is the no-switch counterexample.
        const mainGenerationBeforeSameRelease = readJson<{ nonce: string }>(path.join(config.residencyRoot, "main-generation.json"));
        await rpc.call("same-release");
        expect(readJson<{ nonce: string }>(path.join(config.residencyRoot, "main-generation.json")).nonce)
          .not.toBe(mainGenerationBeforeSameRelease.nonce);
        expect(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).toBe(ownerBytes);
        expect(drainReceipts(config.residencyRoot)).toEqual([]);
        expect(residentProcessAlive(ownerA.pid, ownerA.processStartTime)).toBe(true);

        actor = await rpc.call<FabricActorInfo>("create", { name: "release-switch-queue", scope: "project",
          instructions: "Reply briefly to each input.", residency: "durable", model: "offline-fixture/reply",
          transport: "process", tools: [], extensions: false, delivery: "mailbox", responseMode: "text",
          coalesce: false, topics: ["native.release-switch"], batchWindowMs: 300_000 });
        const first = await rpc.call<{ acknowledged: boolean; messageId?: string }>("followup", {
          id: actor.id, message: "SWITCH_FIRST_ACTIVE_UNTIL_RELEASE",
        });
        expect(first.acknowledged).toBe(true);
        expect(first.messageId).toBeTruthy();
        await observer.until(() => serverRequests.length === 1 && responses.has(1), "A native worker enters its first real Pi inference");
        expect(JSON.stringify(serverRequests[0]!.messages)).toContain("SWITCH_FIRST_ACTIVE_UNTIL_RELEASE");

        const publication = await mesh.publish({ topic: "native.release-switch", to: actor.id,
          from: { id: `${identity.id}:topic-source`, name: "Native topic source", kind: "agent", sessionId: identity.sessionId },
          text: "SWITCH_PENDING_MESH_WAKE", data: { proof: "committed-mesh-event" } });
        await observer.until(() => {
          const directory = path.join(config.actorRoot, actor!.id);
          if (!fs.existsSync(directory)) return false;
          return fs.readdirSync(directory).filter(name => /^queue-.*\.json$/.test(name)).some(name =>
            readJson<QueueSnapshot>(path.join(directory, name)).items.some(item => item.payload?.id === publication.id));
        }, "the committed mesh wake is accepted into A's durable actor queue");
        const eventLine = fs.readFileSync(path.join(meshRoot, "events.jsonl"), "utf8").split("\n")
          .find(line => line && (JSON.parse(line) as { sequence?: number }).sequence === publication.sequence);
        expect(eventLine, "the published wake must have an append-only trace line").toBeDefined();
        const traceBytes = `${eventLine}\n`;
        expect(JSON.parse(eventLine!).id).toBe(publication.id);
        const sessionFile = path.join(config.actorRoot, actor.id, "session.jsonl");
        const sessionBefore = fs.readFileSync(sessionFile);
        expect(sessionBefore.byteLength, "A must have a real raw Pi session trace").toBeGreaterThan(0);

        // Dispose A's client as a runtime reload would, then let B's lifecycle start own the switch.
        await rpc.call("close-a");
        const aLauncherPid = ownerA.handover?.launcher.pid;
        expect(aLauncherPid).toBeTruthy();
        const aLauncherBirth = owned.owned().find(row => row.pid === aLauncherPid);
        expect(aLauncherBirth, "A launcher must have recorded its own native birth").toBeDefined();
        const bGeneration = await rpc.call<ResidentMainGeneration>("switch");
        expect(bGeneration).toMatchObject({ pid: mainChild.pid, processStartTime: mainBirth!.started, releaseRoot: targetRelease });
        expect(readJson<{ releaseRoot: string }>(path.join(config.residencyRoot, "main-generation.json")).releaseRoot)
          .toBe(targetRelease);

        const deferredPath = path.join(config.residencyRoot, "handover-deferred.json");
        await observer.until(() => drainReplies().some(receipt => receipt.code === "DRAIN_EXIT_ACTORS_BUSY") ||
          fs.existsSync(handoverPath(config.residencyRoot)) || fs.existsSync(deferredPath),
        "B startup attempts the native release boundary", 30_000);
        const deferred = fs.existsSync(deferredPath) ? readJson<{ reason?: string }>(deferredPath) : undefined;
        expect(deferred, deferred?.reason ?? "B startup must not defer its release switch").toBeUndefined();
        const firstDrain = drainReplies().find(receipt => receipt.code === "DRAIN_EXIT_ACTORS_BUSY");
        expect(firstDrain, "release startup must use graceful exit drain and report its precommit busy refusal").toMatchObject({
          phase: "refused", outcome: "refused", code: "DRAIN_EXIT_ACTORS_BUSY", admissionHeld: false,
          owner: { pid: ownerA.pid, token: ownerA.token },
        });
        expect(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).toBe(ownerBytes);
        expect(same(ownerBirth!) && processIsLive(ownerA.pid)).toBe(true);
        expect(responses.get(1)!.destroyed || responses.get(1)!.writableEnded).toBe(false);
        expect(serverRequests).toHaveLength(1);

        // Explicit precommit refusal is not permission to kill work. Settle A's real
        // inference naturally; B must then retry exit, read back the durable receipt,
        // and start B before the retained mesh wake is released from its batch fence.
        reply(1);
        await observer.until(() => {
          const row = registries.project.records().find(record => record.id === actor!.id);
          return !!row && (registries.project.messages(row) as FabricActorMessage[]).some(message =>
            message.direction === "out" && message.text === "SWITCH_REPLY_1");
        }, "A naturally completes the already accepted inference");
        await observer.until(() => drainReceipts(config.residencyRoot).some(receipt => receipt.phase === "exited" &&
          receipt.owner.token === ownerA!.token) && fs.existsSync(path.join(config.residencyRoot, "owner.json")) &&
          readJson<ResidentHostOwner>(path.join(config.residencyRoot, "owner.json")).releaseRoot === targetRelease,
        "A exits on its durable receipt and B's real launcher is serving", 180_000);

        const exitReceipt = drainReceipts(config.residencyRoot).find(receipt => receipt.phase === "exited" && receipt.owner.token === ownerA!.token)!;
        const durableReceipt = readJson<ResidentDrainReceipt>(residentDrainReceiptPath(config.residencyRoot, exitReceipt.id));
        expect(durableReceipt).toMatchObject({ id: exitReceipt.id, phase: "exited", outcome: "exited", code: "DRAIN_EXITED",
          terminal: true, admissionHeld: true, checkpointReadback: true,
          owner: { pid: ownerA.pid, processStartTime: ownerA.processStartTime, token: ownerA.token } });
        const stopped = readJson<ResidentGenerationExit>(residentGenerationExitPath(config.residencyRoot, ownerA.token));
        expect(stopped).toMatchObject({ generation: ownerA.token, pid: ownerA.pid, writer: "host", reason: "stopped" });
        await observer.until(() => !processIsLive(ownerA!.pid) && !processIsLive(aLauncherPid!), "both exact A processes exit naturally");
        expect(same(ownerBirth!) && processIsLive(ownerA.pid)).toBe(false);
        expect(same(aLauncherBirth!) && processIsLive(aLauncherPid!)).toBe(false);

        const ownerB = readJson<ResidentHostOwner>(path.join(config.residencyRoot, "owner.json"));
        expect(ownerB).toMatchObject({ releaseRoot: targetRelease, fabricExtensionPath: path.join(targetRelease, "dist/index.js"),
          drain: { abi: "fabric-resident-drain-1" } });
        expect(ownerB.token).not.toBe(ownerA.token);
        expect(ownerB.pid).not.toBe(ownerA.pid);
        expect(residentProcessAlive(ownerB.pid, ownerB.processStartTime)).toBe(true);
        expect(readJson<{ token: string }>(path.join(config.residencyRoot, "maintenance-ready.json")).token).toBe(ownerB.token);
        const bHostBirth = owned.owned().find(row => row.pid === ownerB.pid);
        expect(bHostBirth, "B host must have its own native launch record").toBeDefined();
        expect(bHostBirth!.argv).toContain(path.join(targetRelease, "dist/residency/pi-entry.js"));
        expect(serverRequests).toHaveLength(1);

        const traceAfter = fs.readFileSync(path.join(meshRoot, "events.jsonl"), "utf8").split("\n");
        expect(traceAfter.filter(line => line === eventLine)).toHaveLength(1);
        expect(traceAfter.join("\n")).toContain(traceBytes.trimEnd());
        expect(JSON.stringify(exitReceipt.checkpoint)).toContain(publication.id);
        expect(JSON.stringify(exitReceipt.checkpoint)).toContain("SWITCH_PENDING_MESH_WAKE");
        const actorRow = registries.project.records().find(record => record.id === actor!.id);
        expect(actorRow, "B must see the actor from A's native registry").toBeDefined();
        expect((await rpc.call<FabricActorInfo>("status", { id: actor.id })).id).toBe(actor.id);

        await rpc.call("batching", { id: actor.id });
        await observer.until(() => serverRequests.length === 2 && responses.has(2), "B starts the retained mesh wake once");
        expect(JSON.stringify(serverRequests[1]!.messages)).toContain("SWITCH_PENDING_MESH_WAKE");
        reply(2);
        await observer.until(() => {
          const row = registries.project.records().find(record => record.id === actor!.id);
          if (!row) return false;
          const messages = registries.project.messages(row) as FabricActorMessage[];
          return messages.filter(message => message.direction === "out" && message.text === "SWITCH_REPLY_2").length === 1 &&
            !fs.readdirSync(path.join(config.actorRoot, actor!.id)).filter(name => /^queue-.*\.json$/.test(name))
              .flatMap(name => readJson<QueueSnapshot>(path.join(config.actorRoot, actor!.id, name)).items)
              .some(item => item.payload?.id === publication.id);
        }, "B completes and retires the pending wake");
        const finalRow = registries.project.records().find(record => record.id === actor!.id)!;
        const finalHistory = registries.project.messages(finalRow) as FabricActorMessage[];
        expect(finalHistory.filter(message => message.direction === "in" && message.id === first.messageId)).toHaveLength(1);
        expect(finalHistory.filter(message => message.direction === "out" && message.text === "SWITCH_REPLY_1")).toHaveLength(1);
        expect(finalHistory.filter(message => message.direction === "in" && message.source === "mesh:native.release-switch" &&
          (message.data as { id?: string } | undefined)?.id === publication.id)).toHaveLength(1);
        expect(finalHistory.filter(message => message.direction === "out" && message.text === "SWITCH_REPLY_2")).toHaveLength(1);
        expect(finalHistory.some(message => message.error !== undefined || message.stale === true)).toBe(false);
        expect(serverRequests).toHaveLength(2);
        expect(fs.readFileSync(sessionFile).subarray(0, sessionBefore.byteLength).equals(sessionBefore),
          "B must retain every original byte of A's raw session trace").toBe(true);
        expect(drainReceipts(config.residencyRoot).filter(receipt => receipt.phase === "exited" &&
          receipt.owner.token === ownerA!.token)).toHaveLength(1);
        fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify({ main: bGeneration, ownerA, ownerB, publication,
          first, exitReceipt, drainReplies: drainReplies(), serverRequests, finalHistory,
          sessionPrefixRetained: true }, null, 2));
        await rpc.call("close");
        expect(await mainLifetime.exit).toEqual({ code: 0, signal: null });
      } catch (error) {
        fs.writeFileSync(path.join(root, "failure.json"), JSON.stringify({ error: String(error), mainStderr, serverRequests }, null, 2));
        throw new Error(`${error instanceof Error ? error.stack : error}\nretained native fixture: ${root}\nMain: ${mainStderr}`);
      } finally {
        if (rpc && mainLifetime && !mainLifetime.exited) await rpc.call("close").catch(() => undefined);
        mesh.closeState();
        for (const response of openResponses) if (!response.writableEnded) response.end();
        if (serverListening) await new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); });
        for (const registry of Object.values(registries)) registry.releaseReadCache();
        await stopAllOwned(owned.owned(), 5_000, 5_000);
        const stillLive = owned.owned().filter(row => same(row) && processIsLive(row.pid)).map(row => row.pid);
        if (mainLifetime) await mainLifetime.exit;
        fs.writeFileSync(path.join(root, "cleanup.json"), JSON.stringify({ retained: true, liveFixturePids: stillLive, launches: owned.owned() }, null, 2));
        observer?.close();
        expect(stillLive, "all native processes created by this private fixture must exit").toEqual([]);
        privateTemp.assertNoOutsideDeletion();
        // Optional caller-owned evidence export: retain regular files, including
        // raw sessions, without following dependency symlinks or deleting traces.
        const proofOut = process.env.FABRIC_RESIDENT_SWITCH_PROOF_OUT;
        if (proofOut) {
          const retain = (source: string, target: string): void => {
            fs.mkdirSync(target, { recursive: true, mode: 0o700 });
            for (const entry of fs.readdirSync(source, { withFileTypes: true })) {
              const from = path.join(source, entry.name); const to = path.join(target, entry.name);
              if (entry.isDirectory()) retain(from, to);
              else if (entry.isFile()) fs.writeFileSync(to, fs.readFileSync(from), { flag: "wx", mode: 0o600 });
            }
          };
          retain(root, path.resolve(proofOut));
        }
        vi.unstubAllEnvs();
      }
    }, 240_000);
  });
