import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { setTimeout as sleep } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { CapturedToolCatalog } from "../src/capture/catalog.ts";
import { normalizeFabricConfig } from "../src/config.ts";
import { FabricRuntimeState } from "../src/fabric-runtime-state.ts";
import { AgentMessageRouter } from "../src/providers/agents-message-router.ts";
import { observeFabricPathBProbe } from "../src/transport/probe-hooks.ts";

const configFile = process.argv[2];
if (!configFile) throw new Error("worker config path is required");
const options = JSON.parse(fs.readFileSync(configFile, "utf8"));
const role = options.role;
const projectRoot = options.projectRoot;
const agentDir = options.agentDir;
const meshRoot = options.meshRoot;
const sessionId = randomUUID();
const participantId = `session:${sessionId}`;
const probeEvents = [];
let flushedEvents = 0;
let runtime;
let stopping = false;
let holdUsed = false;

const emit = value => process.stdout.write(`${JSON.stringify(value)}\n`);
const nsNow = () => process.hrtime.bigint().toString();
const epochNsNow = () => (BigInt(Math.round(performance.timeOrigin * 1_000_000)) +
  BigInt(Math.round(performance.now() * 1_000_000))).toString();
const writeProbeEvents = () => {
  if (flushedEvents >= probeEvents.length) return;
  fs.mkdirSync(path.dirname(options.eventLogFile), { recursive: true, mode: 0o700 });
  const text = `${probeEvents.slice(flushedEvents).map(event => JSON.stringify({ ...event, role, hostId: participantId })).join("\n")}\n`;
  fs.appendFileSync(options.eventLogFile, text, { mode: 0o600 });
  flushedEvents = probeEvents.length;
};

const restoreObserver = observeFabricPathBProbe(event => { probeEvents.push(event); });

if (role === "owner" && options.hold?.enabled) {
  const original = AgentMessageRouter.prototype.acceptControl;
  AgentMessageRouter.prototype.acceptControl = async function (command, ...args) {
    if (!holdUsed && command.operation === (options.hold.operation ?? "followUp")) {
      holdUsed = true;
      fs.mkdirSync(path.dirname(options.hold.enteredFile), { recursive: true, mode: 0o700 });
      fs.writeFileSync(options.hold.enteredFile, `${JSON.stringify({ commandId: command.commandId, atNs: nsNow() })}\n`, { mode: 0o600 });
      if (!fs.existsSync(options.hold.releaseFile)) await waitForRelease(options.hold.releaseFile);
    }
    return original.call(this, command, ...args);
  };
}

function waitForRelease(releaseFile) {
  return new Promise((resolve, reject) => {
    if (fs.existsSync(releaseFile)) { resolve(); return; }
    let done = false;
    let watcher;
    let timer;
    const finish = error => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      watcher?.close();
      process.removeListener("SIGUSR1", onSignal);
      error ? reject(error) : resolve();
    };
    const onSignal = () => finish();
    process.once("SIGUSR1", onSignal);
    try {
      watcher = fs.watch(path.dirname(releaseFile), (_event, filename) => {
        if (filename === null || filename.toString() === path.basename(releaseFile)) {
          if (fs.existsSync(releaseFile)) finish();
        }
      });
    } catch (error) { finish(error); return; }
    timer = setTimeout(() => finish(new Error("owner-admission hold exceeded 120 seconds")), 120_000);
    timer.unref();
    if (fs.existsSync(releaseFile)) finish();
  });
}

function runtimeConfig() {
  const supplied = options.runtimeConfig && typeof options.runtimeConfig === "object" ? options.runtimeConfig : {};
  const suppliedMesh = supplied.mesh && typeof supplied.mesh === "object" ? supplied.mesh : {};
  const suppliedAgents = supplied.agents && typeof supplied.agents === "object" ? supplied.agents : {};
  const suppliedResidency = supplied.residency && typeof supplied.residency === "object" ? supplied.residency : {};
  const suppliedMcp = supplied.mcp && typeof supplied.mcp === "object" ? supplied.mcp : {};
  const suppliedMemory = supplied.memory && typeof supplied.memory === "object" ? supplied.memory : {};
  const suppliedJev = supplied.jev && typeof supplied.jev === "object" ? supplied.jev : {};
  const suppliedPrewalk = supplied.prewalk && typeof supplied.prewalk === "object" ? supplied.prewalk : {};
  const mesh = { ...suppliedMesh, enabled: true, transport: options.transport, hostTransport: options.transport,
    root: meshRoot, stateBackend: options.stateBackend, ...(options.nats ? { nats: options.nats } : {}) };
  return normalizeFabricConfig({
    fullCodeMode: false,
    ...supplied,
    mesh,
    agents: { enabled: false, ...suppliedAgents },
    residency: { enabled: false, ...suppliedResidency },
    mcp: { enabled: false, ...suppliedMcp },
    memory: { enabled: false, ...suppliedMemory },
    jev: { enabled: false, ...suppliedJev },
    prewalk: { enabled: false, alwaysRearm: false, ...suppliedPrewalk },
  });
}

function makeMain() {
  const pi = {
    on: () => () => {},
    events: { emit() {}, on: () => () => {} },
    getThinkingLevel: () => "off",
    getSessionName: () => `path-b-probe-${role}`,
    appendEntry() {},
    sendMessage() {},
  };
  const context = {
    cwd: projectRoot,
    hasUI: false,
    mode: "rpc",
    isProjectTrusted: () => true,
    isIdle: () => true,
    hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
    sessionManager: {
      getSessionId: () => sessionId,
      getSessionFile: () => undefined,
      getBranch: () => [],
      getLeafId: () => null,
      getEntries: () => [],
    },
    ui: { setStatus() {}, notify() {} },
  };
  const paths = {
    extension: path.resolve("dist/index.js"),
    worker: path.join(projectRoot, "unused-worker.mjs"),
    residentHost: path.join(projectRoot, "unused-resident.mjs"),
    skills: projectRoot,
  };
  return { pi, context, runtime: new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths }) };
}

function invocation(ref, args) {
  return runtime.registry.invoke(ref, args, {
    cwd: projectRoot,
    signal: undefined,
    parentToolCallId: "nats-path-b-probe",
    nestedToolCallId: ref,
    extensionContext: currentContext,
    update() {},
    approve: async () => {},
    audits: [],
    maxResultChars: 10_000,
  });
}

let currentContext;
async function execute(input) {
  if (input.type === "flush") {
    writeProbeEvents();
    return { flushed: flushedEvents };
  }
  if (input.type === "wait-owner") {
    const deadline = Date.now() + (input.timeoutMs ?? 30_000);
    while (Date.now() < deadline) {
      const found = runtime.participantInfos().find(participant => participant.id === input.ownerId && participant.stale !== true);
      if (found) return { ready: true, participant: found };
      await sleep(25);
    }
    throw new Error(`owner participant did not become live: ${input.ownerId}`);
  }
  if (input.type === "invoke") {
    const ref = input.operation === "ask" ? "agents.ask" : input.operation === "steer" ? "agents.steer" : "agents.followUp";
    const args = { id: input.targetId, message: input.message, idempotencyKey: input.probeId,
      ...(input.data === undefined ? {} : { data: input.data }) };
    const invocationAtNs = nsNow();
    const invocationAtEpochNs = epochNsNow();
    try {
      const result = await invocation(ref, args);
      const acceptedAckObservedAtNs = nsNow();
      const acceptedAckObservedAtEpochNs = epochNsNow();
      const accepted = result?.acknowledged === true && result?.accepted !== false;
      return { probeId: input.probeId, invocationAtNs, invocationAtEpochNs, acceptedAckObservedAtNs,
        acceptedAckObservedAtEpochNs, accepted, result };
    } catch (error) {
      return { probeId: input.probeId, invocationAtNs, invocationAtEpochNs, resolvedAtNs: nsNow(), accepted: false,
        error: error instanceof Error ? error.message : String(error) };
    }
  }
  if (input.type === "shutdown") {
    if (stopping) return { stopped: true };
    stopping = true;
    await runtime.shutdown("exit");
    writeProbeEvents();
    restoreObserver();
    return { stopped: true };
  }
  throw new Error(`unknown worker message: ${String(input.type)}`);
}

try {
  fs.mkdirSync(agentDir, { recursive: true, mode: 0o700 });
  fs.mkdirSync(projectRoot, { recursive: true, mode: 0o700 });
  const { pi, context, runtime: instance } = makeMain();
  runtime = instance;
  currentContext = context;
  await runtime.initialize(context, runtimeConfig());
  const mainJournal = path.join(meshRoot, "main-followups", `${encodeURIComponent(sessionId)}.json`);
  const self = runtime.participantInfos().find(participant => participant.id === participantId);
  emit({ type: "ready", role, sessionId, participantId, meshRoot, agentDir, mainJournal,
    stateBackend: runtime.mesh.stateBackend, transport: runtime.config.mesh.transport,
    ownerHostId: self?.ownerHostId ?? participantId, ownerIdentityId: self?.ownerIdentityId ?? participantId,
    ownerIncarnation: self?.ownerIncarnation ?? "unknown" });
  const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  const activeRequests = new Set();
  for await (const line of input) {
    let request;
    try { request = JSON.parse(line); } catch { emit({ type: "protocol-error", error: "invalid JSON request" }); continue; }
    const requestId = request.requestId;
    if (request.type === "shutdown") {
      await Promise.allSettled([...activeRequests]);
      try { const value = await execute(request); emit({ type: "response", requestId, value }); }
      catch (error) { emit({ type: "response", requestId, error: error instanceof Error ? error.message : String(error) }); }
      break;
    }
    const task = execute(request).then(value => emit({ type: "response", requestId, value }), error =>
      emit({ type: "response", requestId, error: error instanceof Error ? error.message : String(error) }));
    activeRequests.add(task);
    void task.finally(() => activeRequests.delete(task));
  }
  if (!stopping) {
    stopping = true;
    await runtime.shutdown("exit");
    writeProbeEvents();
    restoreObserver();
  }
} catch (error) {
  writeProbeEvents();
  emit({ type: "fatal", error: error instanceof Error ? error.stack ?? error.message : String(error) });
  process.exitCode = 1;
}
