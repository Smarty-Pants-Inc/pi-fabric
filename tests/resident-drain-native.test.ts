import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { findExecutable } from "../src/agents/transports/process-utils.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricActorMessage } from "../src/actors/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { residentRoot, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { ResidentDrainReceipt } from "../src/resident-drain-cli.js";
import { liveControlOwnerIncarnation } from "./helpers/live-control-owner.js";
import { launchLog, same } from "./helpers/owned-processes.js";

const repo = fileURLToPath(new URL("../", import.meta.url));
const live = (pid: number): boolean => {
  try {
    const text = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return text.slice(text.lastIndexOf(")") + 2).split(" ")[0] !== "Z";
  } catch { return false; }
};
// Real temp-root writes drive observation (owner/readiness, participant leases,
// queue/history commits, launcher traces and provider receipts). One deadline is
// a failure bound only: no interval, sleep loop or missed-event polling fallback.
const observeFixture = (root: string) => {
  const listeners = new Set<() => void>();
  const wake = () => { for (const listener of [...listeners]) listener(); };
  let watchError: Error | undefined;
  const watcher = fs.watch(root, { recursive: true, persistent: false }, wake);
  watcher.on("error", error => { watchError = error; wake(); });
  return {
    wake,
    close: () => watcher.close(),
    until: (test: () => boolean, what: string, ms = 60_000): Promise<void> => new Promise((resolve, reject) => {
      const finish = (error?: unknown) => {
        clearTimeout(timer); listeners.delete(check);
        if (error) reject(error); else resolve();
      };
      const check = () => {
        try {
          if (watchError) throw watchError;
          if (test()) finish();
        } catch (error) { finish(error); }
      };
      const timer = setTimeout(() => finish(new Error(`Native resident drain deadline: ${what}`)), ms);
      listeners.add(check); check(); // Subscribe before reading; commits cannot slip past the wait.
    }),
  };
};
const readJson = <T>(file: string): T => JSON.parse(fs.readFileSync(file, "utf8")) as T;
type QueueItem = { id: string; attempts?: number; launchEvidenceVersion?: number; executionStarted?: boolean };
type QueueSnapshot = { format: number; items: QueueItem[] };
const cases = [
  { mode: "canonical", code: "DRAIN_ADMISSION_PROTOCOL_UNAVAILABLE" },
  { mode: "aliased", code: "MISSING_LAUNCH_ATTESTATION" },
] as const;

// Native compiled OLD launcher -> native OLD Pi CLI -> native OLD worker -> native
// OLD Pi CLI. Only provider inference is substituted by loopback HTTP SSE. The
// aliased case deliberately addresses the SAME real worker through a fixture-owned
// symlink outside OLD/dist: residentLaunchSpec genuinely rejects the mixed path,
// so the real host publishes no handover attestation. No OLD artifact or owner,
// lease, actor record, checkpoint or history is manufactured or edited.
// Linux only; fresh `bun run build` is a prerequisite, not a silent skip condition.
// Default CI uses the current build. Pinned-release proof sets both release envs.
// Every fixture root is retained; cleanup signals only exact fixture births.
describe.skipIf(process.platform !== "linux")("compiled resident-drain refuses real native legacy hosts without disturbing accepted work", () => {
  it.each(cases)("$mode real worker refuses $code and remains serving", async ({ mode, code }) => {
    const oldRelease = fs.realpathSync(process.env.FABRIC_DRAIN_TEST_OLD_RELEASE ?? repo);
    const targetRelease = fs.realpathSync(process.env.FABRIC_DRAIN_TEST_NEW_RELEASE ?? repo);
    const launcherPath = path.join(oldRelease, "dist/residency/launcher.js");
    const hostEntry = path.join(oldRelease, "dist/residency/pi-entry.js");
    const realWorker = path.join(oldRelease, "dist/worker.js");
    const piBinary = fs.realpathSync(path.join(oldRelease, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
    const node = findExecutable("node");
    expect(node, "a real Node runtime is required").toBeTruthy();
    for (const artifact of [launcherPath, hostEntry, realWorker, path.join(repo, "dist/resident-drain-cli.js"),
      path.join(repo, "bin/fabric-resident-drain"), path.join(targetRelease, "dist/index.js")]) {
      expect(fs.statSync(artifact).isFile(), `fresh compiled artifact required: ${artifact}`).toBe(true);
    }
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), `resident-drain-native-${mode}-`)));
    const observations = observeFixture(root);
    const until = observations.until;
    const owned = launchLog(root);
    // FABRIC_DRAIN_PROOF_OUT is a JSONL receipt file; exact-byte evidence lives
    // alongside it at <file>.artifacts/<mode>, outside the wrapper's private TMPDIR.
    const proofOut = process.env.FABRIC_DRAIN_PROOF_OUT;
    const evidenceDir = proofOut ? path.join(path.resolve(`${proofOut}.artifacts`), mode) : undefined;
    if (evidenceDir) fs.mkdirSync(evidenceDir, { recursive: true, mode: 0o700 });
    const preserve = (source: string, name: string) => {
      if (evidenceDir && fs.existsSync(source)) fs.copyFileSync(source, path.join(evidenceDir, name));
    };
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir, { mode: 0o700 });
    const wrongRelease = path.join(root, "mismatched-release"); fs.mkdirSync(wrongRelease, { mode: 0o700 });
    const meshRoot = path.join(root, "mesh");
    const identity = { id: `session:drain-native-${mode}`, name: "Main", kind: "main" as const, sessionId: `drain-native-${mode}` };
    const workerPath = mode === "aliased" ? path.join(root, "real-old-worker.js") : realWorker;
    if (mode === "aliased") {
      fs.symlinkSync(realWorker, workerPath);
      expect(fs.realpathSync(workerPath)).toBe(realWorker);
    }
    const providerErrors: string[] = [];
    const requests: Array<{ index: number; method: string | undefined; url: string | undefined; model: string; messages: unknown; tools: unknown }> = [];
    let firstResponse: http.ServerResponse | undefined;
    const pending = new Set<http.ServerResponse>();
    const reply = (response: http.ServerResponse, index: number) => {
      if (response.writableEnded || response.destroyed) return;
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
        id: `drain-offline-${index}`, object: "chat.completion.chunk", created: 1, model: "reply",
        choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      chunk({ role: "assistant", content: index === 1 ? "DRAIN_FIRST_REPLY" : "DRAIN_SECOND_REPLY" });
      chunk({}, "stop"); response.end("data: [DONE]\n\n");
    };
    const server = http.createServer((request, response) => {
      pending.add(response); response.once("close", () => pending.delete(response));
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        try {
          const payload = JSON.parse(body) as { model: string; messages: unknown; tools?: unknown };
          const index = requests.length + 1;
          requests.push({ index, method: request.method, url: request.url, model: payload.model, messages: payload.messages, tools: payload.tools });
          fs.appendFileSync(path.join(root, "provider-requests.jsonl"), JSON.stringify(requests.at(-1)) + "\n");
          if (index === 1) firstResponse = response; // Provider gate, never a fixture-only host pause.
          else reply(response, index);
        } catch (error) {
          providerErrors.push(String(error)); response.writeHead(500); response.end(String(error));
        }
      });
    });
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
    const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, notifyOnComplete: false, retainRuns: true, nice: 19,
        sessionExport: false, timeoutMs: 180_000 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, residentIdleExitMs: 180_000 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath, fabricExtensionPath: path.join(oldRelease, "dist/index.js"), piBinary,
      claudeBinary: "missing-claude", vedaBinary: "missing-veda",
      piModels: { available: [{ provider: "offline-fixture", id: "reply" }], aliases: {}, defaultModel: "offline-fixture/reply" },
      watchdog: { enabled: false } };
    fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
    const configPath = path.join(config.residencyRoot, "config.json"); fs.writeFileSync(configPath, JSON.stringify(config), { mode: 0o600 });
    const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
    participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: identity.name, status: "idle", residency: "session",
      runner: "pi", transport: "host", capabilities: ["fabric", "followUp"], cwd: root, sessionId: identity.sessionId,
      startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
    const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 10_000 });
    const client = new ResidentActorClient(meshRoot, identity.id);
    const registry = new ActorRegistryStore(config.actorRoot);
    // Never inherit this task agent's allowlist, resident custody, fleet mesh,
    // mailbox, profile, role, preloads or placement into the isolated subprocesses.
    // Positive allowlist: unrelated API credentials and SMARTY/fleet state are
    // absent, not merely filtered by a namespace. The only provider credential
    // is the ephemeral loopback key in this fixture's models.json.
    const env: NodeJS.ProcessEnv = Object.fromEntries(["PATH", "TMPDIR", "LANG", "LC_ALL"].flatMap(key =>
      process.env[key] === undefined ? [] : [[key, process.env[key]]]));
    Object.assign(env, owned.env, { NODE_OPTIONS: `--import=${pathToFileURL(path.join(root, "launch-preload.mjs")).href}`,
      HOME: root, PI_OFFLINE: "1", PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_MESH_ROOT: meshRoot,
      PI_FABRIC_PROJECT_ROOT: root, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
      PI_FABRIC_PI_BINARY: piBinary, PI_FABRIC_NODE_BINARY: node });
    let launcher: ReturnType<typeof spawn> | undefined;
    let lifetime: ReturnType<typeof watchResidentChild> | undefined;
    let stderr = "";
    const traces = (): Array<Record<string, unknown>> => {
      try { return fs.readFileSync(path.join(config.residencyRoot, "launcher.log"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)); }
      catch { return []; }
    };
    const ownerFile = path.join(config.residencyRoot, "owner.json");
    const commands: Array<{ expectedRelease: string; stdout: string; stderr: string; exit: { code: number | null; signal: NodeJS.Signals | null }; receipt: ResidentDrainReceipt }> = [];
    const invoke = async (expectedRelease: string) => {
      const child = spawn(node!, [path.join(repo, "bin/fabric-resident-drain"), "--resident", path.basename(config.residencyRoot).slice(0, 12),
        "--mesh-root", meshRoot, "--expected-release", expectedRelease, "--target-release", targetRelease],
      { cwd: root, env, stdio: ["ignore", "pipe", "pipe"] });
      const native = watchResidentChild(child); let stdout = ""; let commandStderr = "";
      child.stdout!.on("data", chunk => { stdout += chunk; }); child.stderr!.on("data", chunk => { commandStderr += chunk; });
      const closed = new Promise<void>((resolve, reject) => { child.once("error", reject); child.once("close", () => resolve()); });
      const timer = setTimeout(() => { void native.stop().catch(() => undefined); }, 20_000);
      try {
        const exit = await native.exit; await closed;
        expect(exit.signal, `CLI must refuse normally, not time out: ${commandStderr}`).toBeNull();
        expect(exit.code).not.toBe(0);
        const lines = stdout.trim().split("\n"); expect(lines).toHaveLength(1);
        const receipt = JSON.parse(lines[0]!) as ResidentDrainReceipt;
        expect(receipt).toMatchObject({ ok: false, phase: "preflight", outcome: "refused", resident: config.residencyRoot,
          expected: expectedRelease, target: targetRelease, unchanged: true, transitionAttempted: false,
          rollback: { status: "not-needed", reason: "no-transition-attempted" } });
        const result = { expectedRelease, stdout, stderr: commandStderr, exit, receipt };
        commands.push(result); fs.writeFileSync(path.join(root, `cli-${commands.length}.json`), JSON.stringify(result, null, 2));
        return receipt;
      } finally { clearTimeout(timer); }
    };
    try {
      await participants.start(); control.start(() => ({ accepted: false }));
      launcher = spawn(node!, [launcherPath, "--config", configPath], { cwd: root, env, stdio: ["ignore", "ignore", "pipe"] });
      lifetime = watchResidentChild(launcher); launcher.on("error", observations.wake);
      void lifetime.exit.then(observations.wake);
      launcher.stderr!.on("data", chunk => { stderr = `${stderr}${chunk}`.slice(-12000); });
      await until(() => fs.existsSync(ownerFile) && fs.existsSync(path.join(config.residencyRoot, "maintenance-ready.json")), "real host readiness");
      const ownerBytes = fs.readFileSync(ownerFile, "utf8"); const owner = JSON.parse(ownerBytes) as ResidentHostOwner;
      preserve(ownerFile, "owner-before.json"); preserve(configPath, "config.json");
      await until(() => owned.owned().some(record => record.pid === owner.pid) && owned.owned().some(record => record.pid === launcher!.pid), "bootstrap birth receipts");
      const ownerBirth = owned.owned().find(record => record.pid === owner.pid)!;
      const launcherBirth = owned.owned().find(record => record.pid === launcher!.pid)!;
      expect(ownerBirth.argv).toContain(piBinary); expect(ownerBirth.argv).toContain(hostEntry);
      expect(owner.processStartTime).toBe(ownerBirth.started);
      expect(launcherBirth.argv).toContain(launcherPath);
      expect(owner.fabricExtensionPath).toBe(path.join(oldRelease, "dist/index.js"));
      if (mode === "canonical") {
        expect(owner.releaseRoot).toBe(oldRelease); expect(owner.configDigest).toMatch(/^[0-9a-f]{64}$/);
        expect(owner.handover).toMatchObject({ abi: "fabric-resident-1", launcher: { pid: launcherBirth.pid,
          processStartTime: launcherBirth.started, entry: hostEntry } });
      } else {
        expect(owner.releaseRoot).toBeUndefined(); expect(owner.configDigest).toBeUndefined(); expect(owner.handover).toBeUndefined();
        expect(traces()).toContainEqual(expect.objectContaining({ event: "handover-capability-deferred", reason: "Resident launch specification mixes releases" }));
      }
      const actor = await client.createActor({ name: `drain-native-${mode}`, instructions: "Reply briefly to each input.", residency: "durable",
        model: "offline-fixture/reply", transport: "process", tools: [], extensions: false, delivery: "mailbox", responseMode: "text", coalesce: false });
      await until(() => !!participants.get(actor.id, Date.now(), { fresh: true }), "actor live presence");
      const first = await control.request(owner.hostId, actor.id, "followUp", { message: "DRAIN_FIRST_INPUT held at provider",
        ownerIncarnation: await liveControlOwnerIncarnation(participants, actor.id) }, owner.hostId);
      expect(first.acknowledged).toBe(true); expect(first.messageId).toBeTruthy();
      await until(() => requests.length === 1 && !!firstResponse && !!participants.get(actor.id, Date.now(), { fresh: true })?.actorRun,
        "actual HTTP inference and actorRun");
      expect(requests[0]).toMatchObject({ method: "POST", model: "reply" });
      expect(requests[0]!.tools ?? []).toEqual([]);
      expect(JSON.stringify(requests[0]!.messages)).toContain("DRAIN_FIRST_INPUT");
      const second = await control.request(owner.hostId, actor.id, "followUp", { message: "DRAIN_SECOND_INPUT acknowledged and queued",
        ownerIncarnation: await liveControlOwnerIncarnation(participants, actor.id) }, owner.hostId);
      expect(second.acknowledged).toBe(true); expect(second.messageId).toBeTruthy(); expect(second.messageId).not.toBe(first.messageId);
      const queueFiles = () => {
        const directory = path.join(config.actorRoot, actor.id);
        try { return fs.readdirSync(directory).filter(name => /^queue-.*\.json$/.test(name)).map(name => path.join(directory, name)).sort(); }
        catch { return []; }
      };
      await until(() => participants.get(actor.id, Date.now(), { fresh: true })?.actorQueued === 1 &&
        queueFiles().some(file => readJson<QueueSnapshot>(file).items.some(item => item.id === second.messageId)), "accepted second input persisted");
      const queuesBefore = queueFiles().map(file => ({ file, bytes: fs.readFileSync(file, "utf8") }));
      const accepted = queuesBefore.flatMap(({ bytes }) => (JSON.parse(bytes) as QueueSnapshot).items);
      expect(accepted.filter(item => item.id === second.messageId)).toEqual([expect.objectContaining({
        id: second.messageId, attempts: 0, launchEvidenceVersion: 1, executionStarted: false })]);
      expect(accepted.filter(item => item.id === first.messageId)).toEqual([expect.objectContaining({
        id: first.messageId, attempts: 0, launchEvidenceVersion: 1, executionStarted: true })]);
      fs.writeFileSync(path.join(root, "queue-before.json"), JSON.stringify(queuesBefore, null, 2));
      const launchesBefore = owned.owned().map(record => `${record.pid}:${record.started}`);
      const spawnedBefore = traces().filter(row => row.event === "child-spawned"); expect(spawnedBefore).toHaveLength(1);
      const unchanged = () => {
        expect(fs.readFileSync(ownerFile, "utf8")).toBe(ownerBytes);
        expect(same(ownerBirth) && live(ownerBirth.pid)).toBe(true);
        expect(same(launcherBirth) && live(launcherBirth.pid)).toBe(true); expect(lifetime!.exited).toBe(false);
        expect(traces().filter(row => row.event === "child-spawned")).toEqual(spawnedBefore);
        // The only new launches may be the two command processes, never a target
        // launcher/native host/worker. Uses fixture bootstrap logs, no fleet scan.
        const extra = owned.owned().filter(record => !launchesBefore.includes(`${record.pid}:${record.started}`));
        expect(extra.every(record => record.argv[0] === path.join(repo, "bin/fabric-resident-drain"))).toBe(true);
        expect(queueFiles().map(file => ({ file, bytes: fs.readFileSync(file, "utf8") }))).toEqual(queuesBefore);
        expect(participants.get(actor.id, Date.now(), { fresh: true })).toMatchObject({ actorQueued: 1 });
        expect(requests).toHaveLength(1);
        expect(fs.existsSync(path.join(config.residencyRoot, "handover.json"))).toBe(false);
        expect(fs.existsSync(path.join(config.residencyRoot, "watchdog-custody.json"))).toBe(false);
      };
      const mismatch = await invoke(wrongRelease);
      expect(mismatch.code).toBe("EXPECTED_RELEASE_MISMATCH"); unchanged();
      const refusal = await invoke(oldRelease);
      expect(refusal.code).toBe(code);
      if (mode === "canonical") {
        expect(refusal.reason).toContain("closes admission and checkpoints accepted work");
        expect(refusal.reason).toContain("setActivationFilter skips/consumes rather than holds");
        expect(refusal.reason).toContain("no transition attempted");
      } else expect(refusal.reason).toContain("Missing immutable releaseRoot");
      unchanged();
      expect(owned.owned().filter(record => !launchesBefore.includes(`${record.pid}:${record.started}`))).toHaveLength(2);
      preserve(ownerFile, "owner-after-refusal.json");
      preserve(path.join(root, "cli-1.json"), "cli-mismatched-release.json");
      preserve(path.join(root, "cli-2.json"), "cli-correct-release.json");
      for (const { file } of queuesBefore) preserve(file, `queue-at-refusal-${path.basename(file)}`);
      reply(firstResponse!, 1);
      const history = (): FabricActorMessage[] => {
        const row = registry.records().find(record => record.id === actor.id);
        return row ? registry.messages(row) as FabricActorMessage[] : [];
      };
      await until(() => requests.length === 2 && history().filter(message => message.direction === "out" && message.text === "DRAIN_SECOND_REPLY").length === 1 &&
        ["idle", "dormant"].includes(participants.get(actor.id, Date.now(), { fresh: true })?.status ?? "") &&
        participants.get(actor.id, Date.now(), { fresh: true })?.actorQueued === 0, "second real native activation completes");
      const status = await client.actorStatus(actor.id);
      expect(["idle", "dormant"]).toContain(status.status); expect(status.queued).toBe(0);
      expect(status.inFlightRun).toBeUndefined(); expect(status.lastError).toBeUndefined();
      const messages = history();
      expect(messages.filter(message => message.direction === "in" && message.id === second.messageId)).toHaveLength(1);
      expect(messages.filter(message => message.direction === "out" && message.text === "DRAIN_SECOND_REPLY")).toHaveLength(1);
      expect(messages.filter(message => message.direction === "out")).toHaveLength(2);
      expect(messages.some(message => message.error !== undefined)).toBe(false);
      expect(requests).toHaveLength(2); expect(JSON.stringify(requests[1]!.messages)).toContain("DRAIN_SECOND_INPUT");
      expect(providerErrors).toEqual([]);
      expect(fs.readFileSync(ownerFile, "utf8")).toBe(ownerBytes);
      expect(same(ownerBirth) && live(ownerBirth.pid)).toBe(true); expect(same(launcherBirth) && live(launcherBirth.pid)).toBe(true);
      expect(lifetime.exited).toBe(false); expect(traces().filter(row => row.event === "child-spawned")).toHaveLength(1);
      expect(queueFiles().flatMap(file => readJson<QueueSnapshot>(file).items).some(item => item.id === second.messageId)).toBe(false);
      const launches = owned.owned();
      expect(launches.filter(record => record.argv[0] === realWorker)).toHaveLength(2);
      expect(launches.filter(record => record.argv[0] === piBinary)).toHaveLength(3); // resident + two activation children
      expect(launches.every(record => [launcherPath, piBinary, realWorker, path.join(repo, "bin/fabric-resident-drain")].includes(record.argv[0] ?? ""))).toBe(true);
      for (const command of commands) expect(command.receipt).toMatchObject({ observed: {
        rootId: identity.id, hostId: owner.hostId, pid: owner.pid, processStartTime: owner.processStartTime, token: owner.token,
        loadedRelease: { releaseRoot: owner.releaseRoot ?? null, fabricExtensionPath: owner.fabricExtensionPath },
      } });
      const proof = { mode, root, oldRelease, targetRelease, command: path.join(repo, "bin/fabric-resident-drain"),
        nativePi: piBinary, workerPath, realWorker, owner: ownerBirth, launcher: launcherBirth, ownerToken: owner.token,
        attested: mode === "canonical", actorId: actor.id, firstAcceptedId: first.messageId, secondAcceptedId: second.messageId,
        queueBefore: accepted, refusals: commands, providerRequests: requests.length, history: messages,
        secondInputCount: messages.filter(message => message.direction === "in" && message.id === second.messageId).length,
        secondOutputCount: messages.filter(message => message.direction === "out" && message.text === "DRAIN_SECOND_REPLY").length,
        actorStatus: status.status, unchangedOwner: true, targetLauncherSpawns: 0, transitionAttempted: false,
        rollback: { status: "not-needed", reason: "no-transition-attempted" }, launches: owned.owned() };
      fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(proof, null, 2));
      fs.writeFileSync(path.join(root, "actor-history.json"), JSON.stringify(messages, null, 2));
      preserve(ownerFile, "owner-after-completion.json");
      preserve(path.join(config.actorRoot, "actors.json"), "actor-registry.json");
      preserve(path.join(root, "actor-history.json"), "actor-history.json");
    } catch (error) {
      let childLog = "";
      try { childLog = fs.readFileSync(path.join(config.residencyRoot, "child-stderr.log"), "utf8").slice(-18000); } catch { /* not started */ }
      fs.writeFileSync(path.join(root, "failure.json"), JSON.stringify({ error: String(error), stderr, childLog, traces: traces(), requests, commands }, null, 2));
      throw new Error(`${error instanceof Error ? error.stack : error}\nretained fixture: ${root}\nlauncher: ${stderr}\nchild: ${childLog}\ntraces: ${JSON.stringify(traces().slice(-12))}`);
    } finally {
      for (const response of pending) if (!response.writableEnded) response.end();
      // Join the direct fixture bootstrap's native exit; any remaining PID signal
      // requires that process's own launch-time receipt and a fresh exact birth.
      try { if (lifetime && !lifetime.exited) await lifetime.stop(); }
      finally {
        for (const record of owned.owned()) if (same(record) && live(record.pid)) {
          try { process.kill(record.pid, "SIGKILL"); } catch { /* exact fixture process exited */ }
        }
        if (lifetime) await lifetime.exit;
        await control.close(); await participants.close(); registry.releaseReadCache();
        await new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); });
        // Native bootstrap stop joins its directly owned tree. For a residual
        // exact-birth SIGKILL above, allow one finite native scheduling turn and
        // read liveness once; no /proc polling or guessed whole-attempt proof.
        await new Promise<void>(resolve => setTimeout(resolve, 1_000));
        const liveFixturePids = owned.owned().filter(record => same(record) && live(record.pid)).map(record => record.pid);
        const cleanup = { retained: true, launches: owned.owned(), liveFixturePids, remainingLiveFixtureCount: liveFixturePids.length };
        observations.close();
        fs.writeFileSync(path.join(root, "cleanup.json"), JSON.stringify(cleanup, null, 2));
        preserve(path.join(root, "cleanup.json"), "cleanup.json");
        expect(liveFixturePids, "only fixture-owned processes must be quiescent after native cleanup").toEqual([]);
        if (fs.existsSync(path.join(root, "proof.json"))) {
          const finalProof = { ...readJson<Record<string, unknown>>(path.join(root, "proof.json")), cleanup };
          fs.writeFileSync(path.join(root, "proof.json"), JSON.stringify(finalProof, null, 2));
          const saved = finalProof as Record<string, unknown>;
          console.info("resident drain offline native refusal proof", JSON.stringify({ mode, root, oldRelease, targetRelease,
            owner: saved.owner, launcher: saved.launcher, ownerToken: saved.ownerToken, actorId: saved.actorId,
            secondAcceptedId: saved.secondAcceptedId, refusalCodes: commands.map(command => command.receipt.code),
            providerRequests: saved.providerRequests, secondInputCount: saved.secondInputCount, secondOutputCount: saved.secondOutputCount,
            actorStatus: saved.actorStatus, unchangedOwner: true, targetLauncherSpawns: 0, transitionAttempted: false,
            remainingLiveFixtureCount: cleanup.remainingLiveFixtureCount, proofOut, evidenceDir }));
          if (proofOut) {
            fs.mkdirSync(path.dirname(path.resolve(proofOut)), { recursive: true });
            fs.appendFileSync(proofOut, JSON.stringify(finalProof) + "\n");
          }
        }
        for (const name of ["proof.json", "cleanup.json", "failure.json", "provider-requests.jsonl", "launches.jsonl", "queue-before.json"]) {
          preserve(path.join(root, name), name);
        }
        preserve(path.join(config.residencyRoot, "launcher.log"), "launcher.log");
        preserve(path.join(config.residencyRoot, "child-stderr.log"), "child-stderr.log");
        // Intentionally NO deletion of root or evidence (local deletion prohibition).
      }
    }
  }, 180_000);
});
